
import neo4j
from neo4j import GraphDatabase
import random
import numpy as np
import py2neo
import pandas as pd

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

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]
    

    def set_spotify_schema(self) -> None:
        """
        Sets the spotify schema and drops the data in the database.
        """
        session = self.driver.session()

        print("breakpoint 1")

        query:str = """
                    LOAD CSV WITH HEADERS FROM 'file:///spotify.csv' AS row
                        CREATE (:Track {
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
                    })
                    """

        session.run(query)

    def flush_database(self) -> None:
        """
        Deletes all nodes and edges from the graph database.
        """
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print(f"Deleted all nodes and edges")


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
            print(len(all_pairs))
            self.sampled_pairs: list = random.sample(all_pairs, batch_size)

    
    def find_recommended_songs(self, track_id: str, num_recommendations=5):
        """
        Given a track ID, finds recommended songs using the specified similarity metric and threshold.
        """
        recommended_songs:np.ndarray = np.zeros()

        # Create the relationships across the entire graph 
        self.evaluate_metrics()
    
        # Get the top recommended songs
        with self.driver.session() as session:
            query: str = f"MATCH (t1:Track)-[r]->(t2:Track) WHERE t1.id = '{track_id}' RETURN t2.id, t2.name ORDER BY r.sim_score DESC LIMIT {num_recommendations}"
            result = session.run(query)
            for record in result:
                recommended_songs.append(record['t2.id'])
        
        return recommended_songs


if __name__ == "__main__":
    print("initialize driver")
    driving: Neo4jDriver = Neo4jDriver("neo4j://localhost:7687", "neo4j", "password")
    driving.connect()
    print("driver working")

    # drop existing database data
    driving.flush_database()
    # fill the db with spotify csv data
    driving.set_spotify_schema()
    driving.sample_pairs()
    print(len(driving.sampled_pairs))
    # driving.disconnect()
     # Find Regina Spektor node
    # regina_nodes = driving.find_node_by_property('Track', 'name', 'Regina Spector')
    # regina_node = regina_nodes[0]
    # regina_id = regina_node['id']

    #  # Find 5 recommended songs for Regina Spektor
    # recommended_songs = driving.find_recommended_songs(regina_id, limit=5)
    # for song in recommended_songs:
    #     print(song)
    


    