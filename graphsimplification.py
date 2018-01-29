"""
Create simplified second graph
"""

from neo4j.v1 import GraphDatabase, basic_auth

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j1"
INPUTGRAPH = "load.cy"

class GraphSimpification(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def prepare_graph(self, file):
        """ loads the graph into the db as a single transaction """
        with self._driver.session() as session:
            session.write_transaction(self.create_graph, file)

    def create_simplified_graph(self):
        """ Creates the simplifed graph as a single transaction """
        with self._driver.session() as session:
            session.write_transaction(self.simplify)

    @staticmethod
    def create_graph(tx, file):
        """ Loads in the graph file into the db """
        tx.run("match (n) detach delete n")
        s = open(file, 'r').read()
        tx.run(s)

    @staticmethod
    def simplify(tx):
        """ Make second graph"""
        tx.run("MATCH (n:NODE) CREATE (x:NODE {name:n.name,type:'simple'})")

        tx.run("""MATCH (n:NODE)-[:AND]->(m:NODE), (x:NODE), (y:NODE)
                    WHERE x.type='simple'AND y.type = 'simple' AND x.name=n.name AND y.name = m.name 
                    MERGE (x)-[:AND]->(y)
                """)
        
        tx.run("""MATCH (n:NODE)-[:OR]->(m:NODE), (x:NODE), (y:NODE)
                    WHERE x.type='simple' AND y.type = 'simple' AND x.name=n.name AND y.name = m.name 
                    MERGE (x)-[:OR]->(y)
                """)

        """Remove unneccesary leaf nodes"""
        tx.run("""START n=NODE(*) MATCH (n)-[r]-() with n, count(r) as c where n.type='simple' and c=1 and n.name starts with 'fact' DETACH DELETE (n)""")

        tx.run("START n=NODE(*) MATCH (n)-[r]-() with n, count(r) as c where n.type='simple' and c=1 and not n.name starts with 'clock' DETACH DELETE (n)")

        tx.run("""START n=NODE(*) MATCH (n)-[r]-() with n, count(r) as c where n.type='simple' and c=1 and n.name ends with "_'])" DETACH DELETE (n)""")

        """ Switch and->or with only one child to and """
        tx.run("""match (n)-[:AND]->(m)-[:OR]->(l) where n.type='simple'  merge (n)-[:AND]->(l)""")
        tx.run("""start n=NODE(*), m=NODE(*) match (n)-[:AND]->(m)-[x:OR]->() with n,m, count(x) as c where n.type='simple' and c=1 detach delete (m)""")
        tx.run("""match (n)-[:AND]->(m)-[:OR]->(l), (n)-[x:AND]->(l) delete x""")

        """ Switch and->or with only one parent to or """
        tx.run("""match (n)-[:AND]->(m)-[:OR]->(l) where n.type='simple' merge (n)-[:OR]->(l)""")
        tx.run("""start n=NODE(*), m=NODE(*) match ()-[x:AND]->(m)-[:OR]->(n) with n,m, count(x) as c where c=1 and n.type='simple' detach delete (m)""")
        tx.run("""match (n)-[:AND]->(m)-[:OR]->(l), (n)-[x:OR]->(l) delete x""")

        """ Switch and->and to and """ 
        tx.run("""match (n)-[:AND]->(m)-[:AND]->(l) where n.type='simple' merge (n)-[:AND]->(l)""")
        tx.run("""match (n)-[:AND]->(m)-[:AND]->(l), (n)-[:AND]->(l) where n.type='simple' detach delete (m)""")

        """ Switch or->and with only one child to or"""
        tx.run("""match (n)-[:OR]->(m)-[:AND]->(l) where n.type='simple'  merge (n)-[:OR]->(l)""")
        tx.run("""start n=NODE(*), m=NODE(*) match (n)-[:OR]->(m)-[x:AND]->() with n,m, count(x) as c where n.type='simple' and c=1 detach delete (m)""")
        tx.run("""match (n)-[:OR]->(m)-[:AND]->(l), (n)-[x:OR]->(l) delete x""")

        # """  Switch or->and with only one parent to and """
        tx.run("""match (n)-[:OR]->(m)-[:AND]->(l) where n.type='simple' merge (n)-[:AND]->(l)""")
        tx.run("""start n=NODE(*), m=NODE(*) match ()-[x:OR]->(m)-[:AND]->(n) with n,m, count(x) as c where c=1 and n.type='simple' detach delete (m)""")
        tx.run("""match (n)-[:OR]->(m)-[:AND]->(l), (n)-[x:AND]->(l) delete x""")

        # """ Switch or->or to just or """
        tx.run("""match (n)-[:OR]->(m)-[:OR]->(l) where n.type='simple' merge (n)-[:OR]->(l)""")
        tx.run("""match (n)-[:OR]->(m)-[:OR]->(l), (n)-[:OR]->(l) where n.type='simple' detach delete (m)""")





if __name__=="__main__":

    # opens the db in question
    gSimplification = GraphSimpification(URI, USERNAME, PASSWORD)

    # inputs a given graph into the db. clears the db
    gSimplification.prepare_graph(INPUTGRAPH)

    # simplifies the graph
    gSimplification.create_simplified_graph()