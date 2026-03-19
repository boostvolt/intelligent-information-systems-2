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
    {entry("node-factory", factory)}
    {entry("node-bundle-name", bundle_name)}
    {entry("node-bundle-symbolic-name", bundle_sym)}
    {entry("node-bundle-vendor", "KNIME GmbH, Konstanz, Germany")}
    {entry("node-bundle-version", "5.1.0")}
    {entry("node-creationTime", "2024-01-01 00:00:00")}
    <config key="model">
{model_inner}
    </config>
</config>
"""


def make_csv_reader(file_hint: str) -> str:
    row_delim = "\\n"
    model = f"""\
        {entry("url", file_hint)}
        {entry("colDelimiter", ";")}
        {entry("rowDelimiter", row_delim)}
        {entry("quote", "&quot;")}
        {entry("commentStart", "")}
        {entry("hasRowHeader", False, "xboolean")}
        {entry("hasColHeader", True, "xboolean")}
        {entry("supportShortLines", False, "xboolean")}
        {entry("limitRowsChecker", False, "xboolean")}
        {entry("skipFirstLinesChecker", False, "xboolean")}
        {entry("characterSetName", "ISO-8859-1")}
        {entry("limitAnalysisChecker", False, "xboolean")}"""
    return node_settings_wrap(
        "CSV Reader",
        "org.knime.base.node.io.csvreader.CSVReaderNodeFactory",
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
    model = f"""\
        <config key="rowFilter">
            {entry("FilterType", "ColValFilter")}
            {entry("ColumnName", column)}
            {entry("Operator", "LE")}
            {entry("MatchCase", False, "xboolean")}
            {entry("DataValue", value)}
            {entry("include", True, "xboolean")}
        </config>"""
    return node_settings_wrap(
        "Row Filter",
        "org.knime.base.node.preproc.filter.row.RowFilterNodeFactory",
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
    model = f"""\
        <config key="col_select">
            {entry("filter-type", "STANDARD")}
            <config key="included_names">
                {entry("array-size", 1, "xint")}
                {entry("0", column)}
            </config>
            <config key="excluded_names">
                {entry("array-size", 0, "xint")}
            </config>
        </config>
        {entry("new_type", "LOCAL_DATE")}
        {entry("format", fmt)}
        {entry("cancel_on_fail", False, "xboolean")}
        {entry("replace_or_append", "replace")}
        {entry("new_col_name_suffix", "_Date")}"""
    return node_settings_wrap(
        "String to Date&amp;Time",
        "org.knime.time.node.convert.stringtodatetime.StringToDateTimeNodeFactory",
        "KNIME Core", "org.knime.core", model)


def make_math_formula(expression: str, result_col: str) -> str:
    model = f"""\
        {entry("expression", expression)}
        {entry("result-column", result_col)}
        {entry("replace-column", True, "xboolean")}"""
    return node_settings_wrap(
        "Math Formula",
        "org.knime.ext.formula.node.processor.FormulatorNodeFactory",
        "KNIME Math Formula", "org.knime.ext.formula", model)


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
mf = add_node("Math Formula ABS Discount", make_math_formula("abs($$Discount$$)", "Discount"),    50+COL_W,  y)
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
