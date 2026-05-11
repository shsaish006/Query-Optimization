# OptiQuery: Advanced SQL Processing and Cost Optimization Engine

## Abstract
OptiQuery is a high-performance analytical tool designed to parse, analyze, and optimize standard SQL queries into Relational Algebra (RA) trees. It employs rigorous heuristic optimization techniques, specifically Predicate Pushdown and Join Reordering, to theoretically minimize query execution costs. By providing a comprehensive comparative analysis of the abstract syntax trees before and after optimization, OptiQuery allows database engineers and developers to visualize execution complexity reductions through a sophisticated web interface.

---

## System Architecture

The following diagram illustrates the advanced structural flow of data and operations within the OptiQuery execution environment.

```mermaid
graph TD
    subgraph Client [Presentation Layer]
        UI[Glassmorphic Web Interface]
        SQL_IN[SQL Query Input]
        METRICS_OUT[Metrics Dashboard Rendering]
        GRAPH_OUT[D3/Graphviz RA Tree Rendering]
    end

    subgraph Controller [Application Router - app.py]
        FLASK[Flask Request Handler]
        ROUTER_INDEX[Base Route]
        ROUTER_PUSH[Predicate Route]
        ROUTER_JOIN[Join Optimization Route]
        ROUTER_COST[Cost Analysis Route]
    end

    subgraph Parser [Parsing Engine - parse.py]
        SQLGLOT[SQLGlot AST Parser]
        RA_BUILDER[Relational Algebra Tree Constructor]
        NODE_REL[Relation Node]
        NODE_SEL[Selection Node]
        NODE_PROJ[Projection Node]
        NODE_JOIN[Join Node]
    end

    subgraph Optimizer [Optimization Engine]
        PRED_PUSH[Predicate Pushdown Optimizer]
        JOIN_OPT[Join Reordering Optimizer]
    end

    subgraph Estimator [Cost Analysis - cost_estimator.py]
        COST_CALC[Recursive Cost Calculator]
        STATS_MGR[Table Statistics Manager]
        METRICS_EXT[Metrics Extraction Matrix]
    end

    %% Flow Definitions
    UI --> SQL_IN
    SQL_IN --> FLASK
    FLASK --> ROUTER_COST
    
    ROUTER_COST --> SQLGLOT
    SQLGLOT --> RA_BUILDER
    RA_BUILDER --> NODE_REL
    RA_BUILDER --> NODE_SEL
    RA_BUILDER --> NODE_JOIN
    RA_BUILDER --> NODE_PROJ
    
    RA_BUILDER --> COST_CALC
    COST_CALC --> STATS_MGR
    
    RA_BUILDER --> PRED_PUSH
    RA_BUILDER --> JOIN_OPT
    
    PRED_PUSH --> COST_CALC
    JOIN_OPT --> COST_CALC
    
    COST_CALC --> METRICS_EXT
    METRICS_EXT --> METRICS_OUT
    RA_BUILDER --> GRAPH_OUT
    
    METRICS_OUT --> UI
    GRAPH_OUT --> UI

    classDef core fill:#2a5298,stroke:#fff,stroke-width:2px,color:#fff;
    classDef sub fill:#1e3c72,stroke:#fff,stroke-width:1px,color:#fff;
    classDef opt fill:#11998e,stroke:#fff,stroke-width:1px,color:#fff;
    
    class Parser,Optimizer,Estimator core;
    class SQLGLOT,RA_BUILDER,PRED_PUSH,JOIN_OPT,COST_CALC sub;
    class METRICS_OUT,GRAPH_OUT opt;
```

---

## Core Operational Modules

### 1. Parsing Engine (`parse.py`)
The system utilizes the `sqlglot` library to generate an Abstract Syntax Tree (AST) from raw SQL strings. This AST is systematically transformed into a Relational Algebra (RA) tree composed of abstract data structures representing fundamental relational operations (Selection, Projection, Join, and Relation).

### 2. Optimization Engine
OptiQuery executes modular optimization routines on the constructed RA tree:
- **Predicate Pushdown (`pred_pushdown.py`)**: Traverses the RA tree to relocate Selection operations as close to the base Relation nodes as possible. This minimizes the cardinality of intermediate datasets processed by subsequent resource-intensive operations, such as Joins.
- **Join Optimization (`join_optimization.py`)**: Analyzes multi-join configurations and reorders the execution sequence to prioritize joins that produce the smallest intermediate computational footprint.

### 3. Cost Estimation Subsystem (`cost_estimator.py`)
The estimator calculates two primary metrics for every node within the RA tree:
- **Operation Cost**: The isolated computational expense of the current node.
- **Cumulative Cost**: The aggregated expense of the current node and all descendant nodes within its specific branch.

Estimations rely on pre-fetched or mocked table row counts to dynamically dictate the cost formulas.

### 4. Application Controller (`app.py`)
The Flask routing matrix serves as the orchestrator. It manages the state of the active RA tree, invokes the parsers and optimizers based on client requests, extracts high-level analytical metrics (e.g., precise node counts, absolute cost disparities), and bridges the logic tier with the rendering tier.

---

## Advanced Presentation Layer

The web interface is engineered using a modern glassmorphic design paradigm layered over a dynamically animated CSS gradient mesh. 

**Analytical Data Displays:**
- **D3 Graphviz Integration**: Renders the complete, interactive Relational Algebra trees directly in the viewport, mapping exact cumulative cost integers to individual procedural nodes.
- **Performance Metrics Dashboard**: A structured grid calculating percentage-based optimization yields, absolute computational reductions, and categorical node distributions (Total Nodes, Total Joins, Base Table allocations).


