
import neo4j
from neo4j import GraphDatabase
import random
import numpy as np


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

        # storage of similarity scores
        self.sim_scores: np.ndarray = np.empty((0,3))
        self.sampled_pairs = None

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

    def set_spotify_schema(self) -> bool:
        """
        Sets the spotify schema and drops the data in the database.
        """
        try:
            with self.driver.session() as session:
                session.run("CREATE INDEX ON :Track(id)")
                session.run("""
                    LOAD CSV WITH HEADERS FROM 'spotify.csv' AS row
                    MERGE (:Track {
                    id: row.track_id,
                    artist: row.artists,
                    album: row.album_name,
                    name: row.track_name,
                    popularity: toInteger(row.popularity),
                    duration_ms: toInteger(row.duration_ms),
                    explicit: toBoolean(row.explicit),
                    danceability: toFloat(row.danceability),
                    energy: toFloat(row.energy),
                    key: toInteger(row.key),
                    loudness: toFloat(row.loudness),
                    mode: toInteger(row.mode),
                    speechiness: toFloat(row.speechiness),
                    acousticness: toFloat(row.acousticness),
                    instrumentalness: toFloat(row.instrumentalness),
                    liveness: toFloat(row.liveness),
                    valence: toFloat(row.valence),
                    tempo: toFloat(row.tempo),
                    time_signature: toInteger(row.time_signature),
                    genre: row.track_genre
                    })-[:BY_ARTIST]->(artist)
                    -[:ON_ALBUM]->(album)
                """)
                session.commit()
                return True
        except:
            return False

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

    def create_relationship(self, start_node_id:str, end_node_id:str, relationship_type:str, props:str) -> None:
        """
        Creates a new relationship between two nodes given their IDs and a relationship type.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query: str = f"MATCH (a),(b) WHERE ID(a)={start_node_id} AND ID(b)={end_node_id} CREATE (a)-[r:{relationship_type} {props}]->(b)"
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
            query: str = f"MATCH ()-[r]-() WHERE ID(r)={relationship_id} DELETE r"
            tx.run(query)
            tx.commit()

    def find_node_by_property(self, label, property_name, property_value) -> list:
        """
        Queries for a given node from a defined property name and value.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query: str = f"MATCH (n:{label}) WHERE n.{property_name}='{property_value}' RETURN n"
            result = tx.run(query)
            records:list = [record["n"] for record in result]
            tx.commit()
            return records

    def get_node_by_id(self, node_id):
        """
        Queries for a given node by its ID.
        """
        with self.driver.session() as session:
            tx = session.begin_transaction()
            query: str = f"MATCH (n) WHERE ID(n)={node_id} RETURN n"
            result = tx.run(query)
            record = result.single()["n"]
            tx.commit()
            return record

    def eucliean_distance(self, track1: str, track2: str) -> float:
        """
        Calculates the similarity score between two tracks based on their attribute values.
        """

        # List of attribute names to use for similarity calculation
        attributes: list[str] = [
            "popularity", "duration_ms", "danceability",
            "energy", "key", "loudness", "mode", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence",
            "tempo", "time_signature"
        ]

        # Calculate Euclidean distance between the tracks
        distance:float = 0.
        for attribute in attributes:
            range_min, range_max = self.driver.get_range(attribute)
            distance += (track1[attribute] - track2[attribute]) ** 2 / ((range_max - range_min) ** 2)

        # Normalize distance to a similarity score between 0 and 1
        similarity:float = 1 / (1 + distance ** 0.5)
        return similarity

    def evaluate_metrics(self, method=eucliean_distance, threshold=.5) -> bool:
        """
        Method for evaluating a given metric threshold over a random batch of nodes.
        """
        if self.sampled_pairs is None:
            self.sample_pairs()

        try:
            for pair in self.sampled_pairs:
                node1 = self.driver.run(f"MATCH (t:Track) WHERE t.id = {pair[0]} USING INDEX t:Track(id) RETURN t.id").single()[0]
                node2 = self.driver.run(f"MATCH (t:Track) WHERE t.id = {pair[1]} USING INDEX t:Track(id) RETURN t.id").single()[0]
                similarity_score: float = method(node1, node2)
                if similarity_score > threshold:
                    self.create_relationship(pair[0], pair[1], "MATCHED", f"{{sim_score: {similarity_score}}}")
            return True
        except:
            return False

    def sample_pairs(self, batch_size=1000) -> None:
        with self.driver.session() as session:
            query:str = "MATCH (t1:Track), (t2:Track) WHERE ID(t1) < ID(t2) RETURN ID(t1), ID(t2)"
            result = session.run(query)
            all_pairs: list = [(record['t1'], record['t2']) for record in result]
            self.sampled_pairs: list = random.sample(all_pairs, batch_size)
    