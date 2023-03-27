
import neo4j
from neo4j import GraphDatabase
import numpy as np
from tqdm import tqdm

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
        self.random_nodes:list = []

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

    def eucliean_distance(self, track1: dict, track2: dict) -> float:
        """
        Calculates the similarity score between two tracks based on their attribute values.
        """

        def process_dict(d) -> np.ndarray:
            # Define a list of keys to exclude from the array
            exclude_keys: list[str] = ['artist', 'album', 'name', 'genre', 'id']

            # Create a list of numerical values, mapping True/False to 1/0
            values:list = []
            for key, value in d.items():
                if key not in exclude_keys:
                    if isinstance(value, bool):
                        values.append(int(value))
                    elif isinstance(value, (int, float)):
                        values.append(value)

            # Convert the list to a NumPy array
            return np.array(values)
        
        # convert the two dicts
        t1: np.ndarray = process_dict(track1)
        t2: np.ndarray = process_dict(track2)

        # Calculate Euclidean distance between the tracks
        return 1/ (np.linalg.norm((t1-t2))+1)

    def evaluate_metrics(self, threshold=.01e-6) -> None:
        """
        Method for evaluating a given metric threshold over a random batch of nodes.
        """
        if len(self.random_nodes)==0:
            self.random_sample()

        with self.driver.session() as session:
            for node1 in tqdm(self.random_nodes):
                for pair_node in range(0, len(self.random_nodes)):
                    node2 = self.random_nodes[pair_node]
                    node1_values:dict = session.run(f"MATCH (t:Track) WHERE ID(t) = {node1} RETURN t").single()['t']._properties
                    
                    if node1 != node2:
                        node2_values:dict = session.run(f"MATCH (t:Track) WHERE ID(t) = {node2} RETURN t").single()['t']._properties
                        similarity_score: float = self.eucliean_distance(node1_values, node2_values)

                        if similarity_score > threshold:
                            self.create_relationship(node1, node2, "MATCHED", f"{{sim_score: {similarity_score}}}")

    def random_sample(self, batch_size=1000) -> None:
        with self.driver.session() as session:
            query:str = f"MATCH (t:Track) WITH t, rand() AS r ORDER BY r RETURN ID(t) AS track_id LIMIT {batch_size}"
            result = session.run(query)
            for record in result:
                self.random_nodes.append(record["track_id"])
    
    def find_recommended_songs(self, track_id: str, num_recommendations=5) -> np.ndarray:
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

    # # drop existing database data
    # driving.flush_database()
    # # fill the db with spotify csv data
    # driving.set_spotify_schema()
    # print("Data dropped")

    # # set randomly sampled tracks
    driving.random_sample(batch_size=1500)
    # print("Sampling complete")

    driving.evaluate_metrics()
    print("metrics evaluated")
    print(driving.random_nodes)
    # driving.find_recommended_songs()
     # Find Regina Spektor node
    # regina_nodes = driving.find_node_by_property('Track', 'name', 'Regina Spector')
    # regina_node = regina_nodes[0]
    # regina_id = regina_node['id']

    #  # Find 5 recommended songs for Regina Spektor
    # recommended_songs = driving.find_recommended_songs(regina_id, limit=5)
    # for song in recommended_songs:
    #     print(song)

    driving.disconnect()



    