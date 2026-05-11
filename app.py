from flask import Flask, request, render_template, url_for
import sqlglot
from sqlglot import expressions as exp
import uuid

from parse import build_ra_tree, visualize_ra_tree, Relation, Selection, Projection, Join, Subquery
from pred_pushdown import pushdown_selections
from cost_estimator import estimate_cost, visualize_costs
from join_optimization import join_optimize
import psycopg2

app = Flask(__name__)

table_stats = None
current_tree = None

def extract_tree_metrics(node):
    metrics = {
        'total_nodes': 0,
        'joins': 0,
        'filters': 0,
        'base_tables': 0
    }
    
    if not node:
        return metrics

    def traverse(n):
        if not n:
            return
        metrics['total_nodes'] += 1
        if isinstance(n, Join):
            metrics['joins'] += 1
        elif isinstance(n, Selection):
            metrics['filters'] += 1
        elif isinstance(n, Relation):
            metrics['base_tables'] += 1
            
        if hasattr(n, 'child') and n.child:
            traverse(n.child)
        if hasattr(n, 'left') and n.left:
            traverse(n.left)
        if hasattr(n, 'right') and n.right:
            traverse(n.right)

    traverse(node)
    return metrics

def get_db_connection():
    # Mocking DB connection so it runs without PostgreSQL
    raise Exception("DB Connection Mocked Out")

def fetch_table_statistics():
    """
    Mock table statistics.
    """
    return {'t1': 1000, 't2': 500}

@app.route('/', methods=['GET', 'POST'])
def index():
    sql = ''
    dot_src = None
    error = None

    if request.method == 'POST':
        sql = request.form.get('sql', '')
        try:
            # Parse the SQL query and build the RA tree

            global table_stats
            global current_tree

            table_stats = fetch_table_statistics()

            current_tree = build_ra_tree(sql)
            estimate_cost(current_tree, table_stats)

            dot_src = visualize_ra_tree(current_tree).source
        except Exception as e:
            error = str(e)

    return render_template('index.html', sql=sql, dot_src=dot_src, error=error)

@app.route('/joinopt', methods=['POST'])
def joinopt():
    """
    Optimize the join order in the relational algebra tree.
    """
    sql = request.form.get('sql', '')
    dot_src = None
    error = None

    try:
        # Perform join optimization on the RA tree

        global table_stats
        global current_tree
    
        estimate_cost(current_tree, table_stats)
        current_tree = join_optimize(current_tree)
        estimate_cost(current_tree, table_stats)

        dot_src = visualize_ra_tree(current_tree).source
    except Exception as e:
        error = str(e)

    return render_template('index.html', sql=sql, dot_src=dot_src, error=error)


@app.route('/pushdown', methods=['POST'])
def pushdown():
    sql = request.form.get('sql', '')
    dot_src = None
    error = None

    try:
        # push down selections in the RA tree
        global table_stats
        global current_tree

        estimate_cost(current_tree, table_stats)
        current_tree = pushdown_selections(current_tree)
        estimate_cost(current_tree, table_stats)

        dot_src = visualize_ra_tree(current_tree).source
    except Exception as e:
        error = str(e)

    return render_template('index.html', sql=sql, dot_src=dot_src, error=error)

@app.route('/cost', methods=['POST'])
def cost():
    sql = request.form.get('sql', '')
    error = None
    ra_tree_svg = None
    ra_tree_cost = 0
    current_tree_svg = None
    current_tree_cost = 0
    comparison_message = None
    comparison_class = None
    ra_metrics = None
    current_metrics = None
    metrics_summary = None
    
    try:
        global table_stats
        global current_tree
        
        ra_tree = build_ra_tree(sql)

        estimate_cost(ra_tree, table_stats)
        ra_tree_svg = visualize_ra_tree(ra_tree).source
        ra_tree_cost = ra_tree.cumulative_cost

        estimate_cost(current_tree, table_stats)
        current_tree_svg = visualize_ra_tree(current_tree).source
        current_tree_cost = current_tree.cumulative_cost

        ra_metrics = extract_tree_metrics(ra_tree)
        current_metrics = extract_tree_metrics(current_tree)

        absolute_diff = ra_tree_cost - current_tree_cost
        pct_improvement = 0
        if ra_tree_cost > 0:
            pct_improvement = (absolute_diff / ra_tree_cost) * 100

        metrics_summary = {
            'absolute_diff': absolute_diff,
            'pct_improvement': pct_improvement
        }

        if ra_tree_cost > (1.001 * current_tree_cost):
            comparison_message = f"Optimized tree is {pct_improvement:.2f}% cheaper!"
            comparison_class = "text-success"
        elif (ra_tree_cost * 1.001) < current_tree_cost:
            comparison_message = f"Optimized tree is {-pct_improvement:.2f}% more expensive!"
            comparison_class = "text-danger"
        else:
            comparison_message = "Both trees have almost the same cumulative cost."
            comparison_class = "text-warning"

    except Exception as e:
        error = str(e)


    return render_template(
        'index.html',
        sql=sql,
        error=error,
        ra_tree_svg=ra_tree_svg,
        current_tree_svg=current_tree_svg,
        ra_tree_cost=ra_tree_cost,
        current_tree_cost=current_tree_cost,
        comparison_message=comparison_message,
        comparison_class=comparison_class,
        ra_metrics=ra_metrics,
        current_metrics=current_metrics,
        metrics_summary=metrics_summary
    )

@app.route('/schema', methods=['GET'])
def get_schema_graph():
    """
    Return a mock schema in DOT format for visualization.
    """
    dot_lines = [
        "digraph Schema {",
        "rankdir=LR;", 
        "node [shape=box, style=filled, color=lightblue, fontname=Consolas];",  
        "edge [fontname=Consolas, color=gray];" 
    ]

    # Mock tables
    tables = {
        't1': ['id (INT)', 'a (VARCHAR)', 'x (INT)'],
        't2': ['id (INT)', 'b (VARCHAR)', 'y (INT)']
    }

    for table_name, columns in tables.items():
        dot_lines.append(
            f'{table_name} [label=<<B>{table_name.upper()}</B><BR ALIGN="LEFT" />' +
            "<BR ALIGN=\"LEFT\" />".join(columns) +
            '>, fillcolor=lightyellow];'
        )

    dot_lines.append('t1 -> t2 [label="id -> id", color=blue];')
    dot_lines.append("}")
    
    return {"dot": "\n".join(dot_lines), "dbname": "Mock DB"}

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
