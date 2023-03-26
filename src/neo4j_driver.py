
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

    def remove_node(self, label: str, node_id: int) -> None:
        """
        Removes a node with the given label and ID from the graph database.

        Args:
            label (str): The label of the node to remove.
            node_id (int): The ID of the node to remove.
        """
        with self.driver.session() as session:
            result = session.run(f"MATCH (n:{label} {{id: {node_id}}}) DETACH DELETE n")
            print(f"Deleted {result.summary().counters.nodes_deleted} nodes and {result.summary().counters.relationships_deleted} edges")

    def remove_edge(self, edge_type: str, start_node_id: int, end_node_id: int) -> None:
        """
        Removes an edge with the given type and start/end node IDs from the graph database.

        Args:
            edge_type (str): The type of the edge to remove.
            start_node_id (int): The ID of the start node of the edge to remove.
            end_node_id (int): The ID of the end node of the edge to remove.
        """
        with self.driver.session() as session:
            result = session.run(f"MATCH (a)-[r:{edge_type}]->(b) WHERE a.id = {start_node_id} AND b.id = {end_node_id} DELETE r")
            print(f"Deleted {result.summary().counters.relationships_deleted} edges")

    def create_node(self, label, **properties):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_node, label, properties)
            return result

    @staticmethod
    def _create_node(tx, label, properties):
        query = f"CREATE (n:{label} {{"
        for key, value in properties.items():
            query += f"{key}: '{value}',"
        query = query[:-1] + "})"
        result = tx.run(query)
        return result
