
import neo4j
from neo4j import GraphDatabase



class Neo4jDriver():
    """
    A class to connect to a Neo4j graph database and perform CRUD operations.

    Attributes:
        uri (str): The URI of the Neo4j server.
        user (str): The username for the Neo4j server.
        password (str): The password for the Neo4j server.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        """
        Constructs a new Neo4jDriver object.

        Args:
            uri (str): The URI of the Neo4j server.
            user (str): The username for the Neo4j server.
            password (str): The password for the Neo4j server.
        """
        self.uri: str = uri
        self.user: str = user
        self.password: str = password
        self.driver: GraphDatabase.driver = None

    def connect(self) -> None:
        """
        Connects to the Neo4j server.
        """
        try:
            self.driver: GraphDatabase.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        except neo4j.exceptions.ServiceUnavailable as e:
            print(f"Failed to connect to Neo4j server: {e}")

    def disconnect(self) -> None:
        """
        Disconnects from the Neo4j server.
        """
        self.driver.close()

    def flush_database(self) -> None:
        """
        Deletes all nodes and edges from the graph database.
        """
        with self.driver.session() as session:
            result = session.run("MATCH (n) DETACH DELETE n")
            print(f"Deleted {result.summary().counters.nodes_deleted} nodes and {result.summary().counters.relationships_deleted} edges")


    def create_node(self, label, **properties):
        """
        Creates a new node using the _create_node() staticmethod.
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._create_node, label, properties)
            return result

    @staticmethod
    def _create_node(tx, label:str, properties:dict):
        """
        Method to create nodes given a dictionary of properties.
        """
        query = f"CREATE (n:{label} {{"
        for key, value in properties.items():
            query += f"{key}: '{value}',"
        query: str = query[:-1] + "})"
        result = tx.run(query)
        return result

    def create_relationship(self, start_node_id:str, end_node_id:str, relationship_type:str) -> None:
        """
        Creates a new relationship between two nodes given their IDs and a relationship type.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query: str = f"MATCH (a),(b) WHERE ID(a)={start_node_id} AND ID(b)={end_node_id} CREATE (a)-[r:{relationship_type}]->(b)"
            tx.run(query)
            tx.commit()

    def delete_node(self, node_id:str) -> None:
        """
        Deletes a node from the database given its ID.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query: str = f"MATCH (n) WHERE ID(n)={node_id} DELETE n"
            tx.run(query)
            tx.commit()

    def delete_relationship(self, relationship_id) -> None:
        """
        Deletes a relationship from the database given the relationship ID.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query = f"MATCH ()-[r]-() WHERE ID(r)={relationship_id} DELETE r"
            tx.run(query)
            tx.commit()

    def find_node_by_property(self, label, property_name, property_value) -> list:
        """
        Queries for a given node from a defined property name and value.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query = f"MATCH (n:{label}) WHERE n.{property_name}='{property_value}' RETURN n"
            result = tx.run(query)
            records = [record["n"] for record in result]
            tx.commit()
            return records

    def get_node_by_id(self, node_id):
        """
        Queries for a given node by its ID.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query = f"MATCH (n) WHERE ID(n)={node_id} RETURN n"
            result = tx.run(query)
            record = result.single()["n"]
            tx.commit()
            return record