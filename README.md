# Query-Optimization
OptiQuery is a simple SQL query processing and optimizing tool. It takes input an SQL query and builds the relational algebra tree corresponding to it. After doing so, certain query optimization techniques like predicate pushdown and join optimization can be performed on it. The difference in estimated cost to run the query can be seen in the user interface.

Setup Instructions
Setup PostgreSQL on your machine and create a database for running the application.
Download the TPC-H dataset from the instructions given here
Run all the commands given in init-tpch.txt in the database environment sequentially.
Setup python virtualenv:
python3 -m venv optiquery
pip install -r requirements.txt
Modify the database information as per your local setup in app.py
Running
Run the following command to start the application.

source optiquery/bin/activate
python3 app.py
