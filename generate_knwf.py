#!/usr/bin/env python3
"""
Generate a KNIME workflow (.knwf) for the Northwind staging ETL.

Uses the correct KNIME config XML format (key="workflow.knime" style).
After import into KNIME, update the PostgreSQL Connector credentials and
CSV Reader file paths, then execute.
"""

import os
import shutil
import zipfile

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_NAME = "staging_etl"
WORKFLOW_DIR  = os.path.join(SCRIPT_DIR, "knime_workflow")
KNWF_PATH     = os.path.join(SCRIPT_DIR, "staging_etl.knwf")

# Always start clean to avoid stale node folders from previous runs
if os.path.exists(WORKFLOW_DIR):
    shutil.rmtree(WORKFLOW_DIR)
os.makedirs(WORKFLOW_DIR)

# ---------------------------------------------------------------------------
# XML config helpers
# ---------------------------------------------------------------------------
XML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'
CONFIG_NS  = ('xmlns="http://www.knime.org/2008/09/XMLConfig" '
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
              'xsi:schemaLocation="http://www.knime.org/2008/09/XMLConfig '
              'http://www.knime.org/XMLConfig_2008_09.xsd"')


def entry(key: str, value, typ: str = "xstring", isnull: bool = False) -> str:
    if isnull:
        return f'<entry key="{key}" type="{typ}" isnull="true" value=""/>'
    if typ == "xboolean":
        value = "true" if value else "false"
    elif typ == "xint":
        value = str(int(value))
    return f'<entry key="{key}" type="{typ}" value="{value}"/>'


def cfg(key: str, *children: str) -> str:
    inner = "\n".join(children)
    return f'<config key="{key}">\n{inner}\n</config>'


# ---------------------------------------------------------------------------
# Node counter
# ---------------------------------------------------------------------------
_nid = 0

def nid() -> int:
    global _nid
    _nid += 1
    return _nid


# ---------------------------------------------------------------------------
# Per-node settings.xml content builders
# ---------------------------------------------------------------------------
BUNDLE_SUFFIX = ('    ' + entry("node-bundle-vendor",
                                "KNIME GmbH, Konstanz, Germany") + '\n'
                 '    ' + entry("node-bundle-version", "5.1.0") + '\n'
                 '    ' + entry("node-creationTime", "2024-01-01 00:00:00"))


def node_settings_wrap(name: str, factory: str, bundle_name: str,
                       bundle_sym: str, model_inner: str) -> str:
    return f"""\
{XML_HEADER}
<config {CONFIG_NS} key="settings.xml">
    {entry("node-name", name)}
    {entry("factory", factory)}
    {entry("node-bundle-name", bundle_name)}
    {entry("node-bundle-symbolic-name", bundle_sym)}
    {entry("node-bundle-vendor", "KNIME GmbH, Konstanz, Germany")}
    {entry("node-bundle-version", "5.1.0")}
    {entry("node-creationTime", "2024-01-01 00:00:00")}
    {entry("customDescription", "", isnull=True)}
    {entry("state", "IDLE")}
    <config key="factory_settings"/>
    <config key="model">
{model_inner}
    </config>
    <config key="ports"/>
    <config key="internal_node_subsettings">
        {entry("memory_policy", "CacheSmallInMemory")}
    </config>
</config>
"""


def make_csv_reader(file_path: str) -> str:
    """Non-deprecated CSV Reader (CSVTableReaderNodeFactory, KNIME 4.3+)."""
    model = f"""\
        <config key="settings">
            <config key="file_selection_Internals">
                <entry key="SettingsModelID" type="xstring" value="SMID_ReaderFileChooser"/>
                <entry key="EnabledStatus" type="xboolean" value="true"/>
            </config>
            <config key="file_selection">
                <config key="file_system_chooser__Internals">
                    <entry key="has_fs_port" type="xboolean" value="false"/>
                    <entry key="overwritten_by_variable" type="xboolean" value="false"/>
                    <entry key="convenience_fs_category" type="xstring" value="LOCAL"/>
                    <entry key="relative_to" type="xstring" value="knime.workflow.data"/>
                    <entry key="mountpoint" type="xstring" value="LOCAL"/>
                    <entry key="spaceId" type="xstring" value=""/>
                    <entry key="spaceName" type="xstring" value=""/>
                    <entry key="custom_url_timeout" type="xint" value="1000"/>
                    <entry key="connected_fs" type="xboolean" value="false"/>
                </config>
                <config key="path">
                    <entry key="location_present" type="xboolean" value="true"/>
                    <entry key="file_system_type" type="xstring" value="LOCAL"/>
                    <entry key="file_system_specifier" type="xstring" value=""/>
                    <entry key="path" type="xstring" value="{file_path}"/>
                </config>
                <config key="filter_mode_Internals">
                    <entry key="SettingsModelID" type="xstring" value="SMID_FilterMode"/>
                    <entry key="EnabledStatus" type="xboolean" value="true"/>
                </config>
                <config key="filter_mode">
                    <entry key="filter_mode" type="xstring" value="FILE"/>
                    <entry key="include_subfolders" type="xboolean" value="false"/>
                    <config key="filter_options">
                        <entry key="filter_files_extension" type="xboolean" value="false"/>
                        <entry key="files_extension_expression" type="xstring" value=""/>
                        <entry key="files_extension_case_sensitive" type="xboolean" value="false"/>
                        <entry key="filter_files_name" type="xboolean" value="false"/>
                        <entry key="files_name_expression" type="xstring" value="*"/>
                        <entry key="files_name_case_sensitive" type="xboolean" value="false"/>
                        <entry key="files_name_filter_type" type="xstring" value="WILDCARD"/>
                        <entry key="include_hidden_files" type="xboolean" value="false"/>
                        <entry key="include_special_files" type="xboolean" value="true"/>
                        <entry key="filter_folders_name" type="xboolean" value="false"/>
                        <entry key="folders_name_expression" type="xstring" value="*"/>
                        <entry key="folders_name_case_sensitive" type="xboolean" value="false"/>
                        <entry key="folders_name_filter_type" type="xstring" value="WILDCARD"/>
                        <entry key="include_hidden_folders" type="xboolean" value="false"/>
                        <entry key="follow_links" type="xboolean" value="true"/>
                    </config>
                </config>
            </config>
            <entry key="has_column_header" type="xboolean" value="true"/>
            <entry key="has_row_id" type="xboolean" value="false"/>
            <entry key="support_short_data_rows" type="xboolean" value="false"/>
            <entry key="skip_empty_data_rows" type="xboolean" value="false"/>
            <entry key="prepend_file_idx_to_row_id" type="xboolean" value="false"/>
            <entry key="comment_char" type="xstring" value="#"/>
            <entry key="column_delimiter" type="xstring" value=";"/>
            <entry key="quote_char" type="xstring" value="&quot;"/>
            <entry key="quote_escape_char" type="xstring" value="&quot;"/>
            <entry key="use_line_break_row_delimiter" type="xboolean" value="true"/>
            <entry key="row_delimiter" type="xstring" value="%%00013%%00010"/>
            <entry key="autodetect_buffer_size" type="xint" value="1048576"/>
        </config>
        <config key="advanced_settings">
            <entry key="spec_merge_mode_Internals" type="xstring" value="UNION"/>
            <entry key="fail_on_differing_specs" type="xboolean" value="true"/>
            <entry key="append_path_column_Internals" type="xboolean" value="false"/>
            <entry key="path_column_name_Internals" type="xstring" value="Path"/>
            <entry key="limit_data_rows_scanned" type="xboolean" value="true"/>
            <entry key="max_data_rows_scanned" type="xlong" value="10000"/>
            <entry key="save_table_spec_config_Internals" type="xboolean" value="true"/>
            <entry key="check_table_spec" type="xboolean" value="false"/>
            <entry key="limit_memory_per_column" type="xboolean" value="true"/>
            <entry key="maximum_number_of_columns" type="xint" value="8192"/>
            <entry key="quote_option" type="xstring" value="REMOVE_QUOTES_AND_TRIM"/>
            <entry key="replace_empty_quotes_with_missing" type="xboolean" value="true"/>
            <entry key="no_row_delimiters_in_quotes" type="xboolean" value="false"/>
            <entry key="min_chunk_size_in_bytes" type="xlong" value="67108864"/>
            <entry key="max_num_chunks_per_file" type="xint" value="4"/>
            <entry key="thousands_separator" type="xstring" value="%%00000"/>
            <entry key="decimal_separator" type="xstring" value="."/>
        </config>
        <config key="limit_rows">
            <entry key="skip_lines" type="xboolean" value="false"/>
            <entry key="number_of_lines_to_skip" type="xlong" value="1"/>
            <entry key="skip_data_rows" type="xboolean" value="false"/>
            <entry key="number_of_rows_to_skip" type="xlong" value="1"/>
            <entry key="limit_data_rows" type="xboolean" value="false"/>
            <entry key="max_rows" type="xlong" value="50"/>
        </config>
        <config key="encoding">
            <entry key="charset" type="xstring" value="ISO-8859-1"/>
        </config>"""
    return node_settings_wrap(
        "CSV Reader",
        "org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory",
        "KNIME Base Nodes", "org.knime.base", model)


def make_db_connector() -> str:
    model = f"""\
        {entry("database_type", "postgresql")}
        {entry("hostname", "localhost")}
        {entry("port", 5432, "xint")}
        {entry("database_name", "northwind")}
        {entry("username", "postgres")}
        {entry("password", "")}"""
    return node_settings_wrap(
        "PostgreSQL Connector",
        "org.knime.database.node.connector.server.DBServerConnectorNodeFactory",
        "KNIME Database", "org.knime.database", model)


def make_db_writer(schema: str, table: str) -> str:
    model = f"""\
        {entry("table", f"{schema}.{table}")}
        {entry("append", False, "xboolean")}
        {entry("insert-null-for-missing-cols", True, "xboolean")}
        {entry("fail-if-exception", True, "xboolean")}
        {entry("batch-size", 1000, "xint")}"""
    return node_settings_wrap(
        "DB Writer",
        "org.knime.database.node.io.writer.DBWriterNodeFactory",
        "KNIME Database", "org.knime.database", model)


def make_row_filter(column: str, value: str) -> str:
    """Non-deprecated Row Filter (row3.RowFilterNodeFactory, KNIME 5.x)."""
    model = f"""\
        <entry key="outputMode" type="xstring" value="MATCHING"/>
        <entry key="matchCriteria" type="xstring" value="AND"/>
        <config key="predicates">
            <config key="0">
                <config key="column">
                    <entry key="selected" type="xstring" value="{column}"/>
                    <config key="compatibleTypes_Internals">
                        <entry key="array-size" type="xint" value="1"/>
                        <entry key="0" type="xstring" value="org.knime.core.data.DoubleValue"/>
                    </config>
                </config>
                <entry key="operator" type="xstring" value="LTE"/>
                <config key="predicateValues">
                    <config key="values">
                        <config key="0">
                            <config key="typeIdentifier">
                                <entry key="cell_class" type="xstring" value="org.knime.core.data.def.DoubleCell"/>
                                <entry key="is_null" type="xboolean" value="false"/>
                            </config>
                            <entry key="value" type="xstring" value="{value}"/>
                        </config>
                    </config>
                    <entry key="inputKind" type="xstring" value="SINGLE"/>
                </config>
            </config>
        </config>
        <entry key="domains" type="xstring" value="RETAIN"/>"""
    return node_settings_wrap(
        "Row Filter",
        "org.knime.base.node.preproc.filter.row3.RowFilterNodeFactory",
        "KNIME Base Nodes", "org.knime.base", model)


def make_string_manip(expression: str, replaced_col: str) -> str:
    # escape XML special chars in expression
    expr_xml = (expression
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;"))
    model = f"""\
        {entry("expression", expr_xml)}
        {entry("replace-or-append", "replace")}
        {entry("replaced-column", replaced_col)}
        {entry("new-column-name", replaced_col)}"""
    return node_settings_wrap(
        "String Manipulation",
        "org.knime.base.node.preproc.stringmanipulation.StringManipulationNodeFactory",
        "KNIME Base Nodes", "org.knime.base", model)


def make_str_to_date(column: str, fmt: str) -> str:
    """Non-deprecated String to Date&Time (NodeFactory2, WebUI node).
    NOTE: NodeFactory2 is a WebUI node — column selection and format must be
    configured manually in KNIME after import (open the node dialog).
    Expected column: {column}, format: {fmt}
    """
    # NodeFactory2 uses the KNIME WebUI settings system.
    # Provide empty model; user sets column + format in the dialog.
    model = ""
    return node_settings_wrap(
        "String to Date&amp;Time",
        "org.knime.time.node.convert.stringtodatetime.StringToDateTimeNodeFactory2",
        "KNIME Date and Time Handling", "org.knime.time", model)


def make_rule_engine_abs(column: str) -> str:
    """Rule Engine node (built-in) to take ABS of a numeric column."""
    rule0 = f"${column}$ &lt; 0 =&gt; -${column}$"
    rule1 = f"TRUE =&gt; ${column}$"
    model = f"""\
        <config key="rules">
            {entry("array-size", 2, "xint")}
            {entry("0", rule0)}
            {entry("1", rule1)}
        </config>
        {entry("append-column", "replace")}
        {entry("new-col-name", column)}
        {entry("error-on-no-match", True, "xboolean")}"""
    return node_settings_wrap(
        "Rule Engine",
        "org.knime.base.node.rules.engine.RuleEngineNodeFactory",
        "KNIME Base Nodes", "org.knime.base", model)


def make_missing_value(column: str, default_val: str) -> str:
    model = f"""\
        <config key="columnSettings">
            <config key="0">
                {entry("columnName", column)}
                {entry("method", "FIX_VALUE")}
                {entry("fixedValue", default_val)}
            </config>
        </config>"""
    return node_settings_wrap(
        "Missing Value",
        "org.knime.base.node.preproc.pmml.missingval.MVImputationNodeFactory",
        "KNIME Base Nodes", "org.knime.base", model)


# ---------------------------------------------------------------------------
# Node registry
# Each entry: (node_id, folder_name, settings_xml)
# ---------------------------------------------------------------------------
nodes: list[tuple[int, str, str]] = []
# connections: (src_id, src_port, dst_id, dst_port)
connections: list[tuple[int, int, int, int]] = []

# positions: node_id -> (x, y)
positions: dict[int, tuple[int, int]] = {}


def add_node(label: str, settings: str, x: int, y: int) -> int:
    node_id = nid()
    folder  = f"{label} (#{node_id})"
    nodes.append((node_id, folder, settings))
    positions[node_id] = (x, y)
    return node_id


def connect(src: int, dst: int, src_port: int = 1, dst_port: int = 1):
    connections.append((src, src_port, dst, dst_port))


# ---------------------------------------------------------------------------
# Build the pipeline nodes
# ---------------------------------------------------------------------------
ROW_H = 160   # vertical spacing between pipelines
COL_W = 200   # horizontal spacing between nodes
CX    = 1400  # x position of shared connector

conn = add_node("PostgreSQL Connector", make_db_connector(), CX, 50)

# --- Categories (row 0) ---
y = 50
n = add_node("CSV Reader Categories",      make_csv_reader("/path/to/Categories.csv"), 50,        y)
w = add_node("DB Writer staging.categories", make_db_writer("staging", "categories"),  50+COL_W,  y)
connect(n, w, 1, 1);  connect(conn, w, 1, 2)

# --- Customers (row 1) ---
y += ROW_H
n  = add_node("CSV Reader Customers",            make_csv_reader("/path/to/Customers.csv"), 50,         y)
s1 = add_node("String Manipulation Region NULL", make_string_manip('regexReplace($Region$,"^NULL$","")', "Region"), 50+COL_W,   y)
s2 = add_node("String Manipulation Fax NULL",    make_string_manip('regexReplace($Fax$,"^NULL$","")',    "Fax"),    50+COL_W*2, y)
w  = add_node("DB Writer staging.customers",     make_db_writer("staging", "customers"),               50+COL_W*3, y)
connect(n, s1); connect(s1, s2); connect(s2, w); connect(conn, w, 1, 2)

# --- Products (row 2) ---
y += ROW_H
n  = add_node("CSV Reader Products",         make_csv_reader("/path/to/Products.csv"), 50,        y)
rf = add_node("Row Filter ProductID le 77",  make_row_filter("ProductID", "77"),       50+COL_W,  y)
w  = add_node("DB Writer staging.products",  make_db_writer("staging", "products"),    50+COL_W*2, y)
connect(n, rf); connect(rf, w); connect(conn, w, 1, 2)

# --- Orders (row 3) ---
y += ROW_H
n   = add_node("CSV Reader Orders",                 make_csv_reader("/path/to/Orders.csv"),           50,         y)
sm1 = add_node("String Manipulation ShipRegion",    make_string_manip('regexReplace($ShipRegion$,"^NULL$","")','ShipRegion'), 50+COL_W,   y)
sm2 = add_node("String Manipulation ShippedDate",   make_string_manip('regexReplace($ShippedDate$,"^NULL$","")','ShippedDate'), 50+COL_W*2, y)
d1  = add_node("String to DateTime OrderDate",      make_str_to_date("OrderDate",    "MM-dd-yyyy"),   50+COL_W*3, y)
d2  = add_node("String to DateTime RequiredDate",   make_str_to_date("RequiredDate", "MM-dd-yyyy"),   50+COL_W*4, y)
d3  = add_node("String to DateTime ShippedDate",    make_str_to_date("ShippedDate",  "MM-dd-yyyy"),   50+COL_W*5, y)
w   = add_node("DB Writer staging.orders",          make_db_writer("staging", "orders"),              50+COL_W*6, y)
connect(n,sm1); connect(sm1,sm2); connect(sm2,d1); connect(d1,d2); connect(d2,d3); connect(d3,w)
connect(conn, w, 1, 2)

# --- OrderDetails (row 4) ---
y += ROW_H
n  = add_node("CSV Reader OrderDetails", make_csv_reader("/path/to/OrderDetails.csv"), 50,        y)
mf = add_node("Rule Engine ABS Discount",  make_rule_engine_abs("Discount"),                       50+COL_W,  y)
mv = add_node("Missing Value Discount 0",  make_missing_value("Discount", "0"),                   50+COL_W*2, y)
w  = add_node("DB Writer staging.order_details", make_db_writer("staging", "order_details"),      50+COL_W*3, y)
connect(n, mf); connect(mf, mv); connect(mv, w); connect(conn, w, 1, 2)

# --- Employees (row 5) ---
y += ROW_H
n  = add_node("CSV Reader Employees",       make_csv_reader("/path/to/Employees.csv"), 50,        y)
d1 = add_node("String to DateTime BirthDate", make_str_to_date("BirthDate", "dd-MM-yyyy"),        50+COL_W,  y)
d2 = add_node("String to DateTime HireDate",  make_str_to_date("HireDate",  "dd-MM-yyyy"),        50+COL_W*2, y)
w  = add_node("DB Writer staging.employees",  make_db_writer("staging", "employees"),             50+COL_W*3, y)
connect(n, d1); connect(d1, d2); connect(d2, w); connect(conn, w, 1, 2)


# ---------------------------------------------------------------------------
# Build workflow.knime
# ---------------------------------------------------------------------------
newline = "\n"
node_configs = []
for i, (node_id, folder, _) in enumerate(nodes):
    x, y = positions[node_id]
    node_configs.append(f"""\
        <config key="node_{node_id}">
            {entry("id", node_id, "xint")}
            {entry("node_settings_file", f"{folder}/settings.xml")}
            {entry("node_is_meta", False, "xboolean")}
            {entry("node_type", "NativeNode")}
            {entry("ui_classname", "org.knime.core.node.workflow.NodeUIInformation")}
            <config key="ui_settings">
                <config key="extrainfo.node.bounds">
                    {entry("array-size", 4, "xint")}
                    {entry("0", x, "xint")}
                    {entry("1", y, "xint")}
                    {entry("2", 120, "xint")}
                    {entry("3", 80, "xint")}
                </config>
            </config>
        </config>""")

conn_configs = []
for i, (src, src_p, dst, dst_p) in enumerate(connections):
    conn_configs.append(f"""\
        <config key="connection_{i}">
            {entry("sourceID", src, "xint")}
            {entry("destID",   dst, "xint")}
            {entry("sourcePort", src_p, "xint")}
            {entry("destPort",   dst_p, "xint")}
            {entry("ui_classname", "org.knime.core.node.workflow.ConnectionUIInformation")}
            <config key="ui_settings"/>
        </config>""")

workflow_xml = f"""\
{XML_HEADER}
<config {CONFIG_NS} key="workflow.knime">
    {entry("version", "5.1.0")}
    {entry("name", WORKFLOW_NAME)}
    <config key="nodes">
{newline.join(node_configs)}
    </config>
    <config key="connections">
{newline.join(conn_configs)}
    </config>
    <config key="workflow_credentials"/>
    <config key="workflow_variables"/>
    {entry("customDescription", "", isnull=True)}
    {entry("state", "IDLE")}
</config>
"""

# ---------------------------------------------------------------------------
# Write files
# ---------------------------------------------------------------------------
with open(os.path.join(WORKFLOW_DIR, "workflow.knime"), "w", encoding="utf-8") as f:
    f.write(workflow_xml)

for node_id, folder, settings in nodes:
    nd = os.path.join(WORKFLOW_DIR, folder)
    os.makedirs(nd, exist_ok=True)
    with open(os.path.join(nd, "settings.xml"), "w", encoding="utf-8") as f:
        f.write(settings)

# ---------------------------------------------------------------------------
# Package as .knwf  (ZIP with workflow folder at root)
# ---------------------------------------------------------------------------
with zipfile.ZipFile(KNWF_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(WORKFLOW_DIR):
        for fname in files:
            full = os.path.join(root, fname)
            # archive path: workflow_name/...
            rel  = os.path.relpath(full, SCRIPT_DIR)
            # rename knime_workflow/ → staging_etl/ inside the archive
            arc  = rel.replace("knime_workflow", WORKFLOW_NAME, 1)
            zf.write(full, arc)

print(f"Generated {len(nodes)} nodes, {len(connections)} connections")
print(f"Archive:  {KNWF_PATH}")
print()
print("Next steps:")
print("  1. KNIME → File → Import KNIME Workflow → select staging_etl.knwf")
print("  2. Open 'PostgreSQL Connector' → set host / database / user / password")
print("  3. Open each 'CSV Reader' → set absolute file path")
print("  4. Execute workflow")
