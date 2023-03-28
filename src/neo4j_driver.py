
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

        # storage of random and artist nodes
        self.random_nodes:list = []
        self.artists_nodes: list = []

        # track keys and exclude keys for sim score computation
        self.track_keys: list[str] = ['id', 'artist', 'album', 'name', 'popularity', 'duration_ms', 'explicit',
                                      'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                                      'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
                                      'time_signature', 'genre']

        self.exclude_keys: list[str] = ['artist', 'album', 'name', 'genre', 'id', 'explicit']

        # max and min values for each key, evalutated later
        self.max_min_values: dict = {}

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

    def set_spotify_schema(self) -> None:
        """
        Sets the spotify schema and drops the data in the database.
        """
        session = self.driver.session()

        # defines the schema to drop from the csv file
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
        # executes the query
        session.run(query)

    def flush_database(self) -> None:
        """
        Deletes all nodes and edges from the graph database.
        """
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print(f"Deleted all nodes and edges")


    def create_relationship(self, start_node_id:str, end_node_id:str, relationship_type:str, props:str) -> None:
        """
        Creates a new relationship between two nodes given their IDs and a relationship type.

        Args:
            start_node_id (str): The ID of the start node.
            end_node_id (str): The ID of the end node.
            relationship_type (str): The type of relationship to create.
            props (str): The properties of the relationship.
        """
        with self.driver.session() as session:
            # beings transaction
            tx = session.begin_transaction()
            # query string
            query: str = f"MATCH (a),(b) WHERE ID(a)={start_node_id} AND ID(b)={end_node_id} CREATE (a)-[r:{relationship_type} {props}]->(b)"
            # executes and commits
            tx.run(query)
            tx.commit()

    def eucliean_distance(self, track1: dict, track2: dict) -> float:
        """
        Calculates the similarity score between two tracks based on their attribute values.

        Args:
            track1 (dict): The first track.
            track2 (dict): The second track.

        Returns:
            float: The similarity score between the two tracks.
        """

        def process_dict(d) -> np.ndarray:
            """
            Processes input dictionaries to scrape numerical and boolean values to add to sim score vector.
            """
            values:list = []
            for key, value in d.items():
                if key not in self.exclude_keys: # ignore element if it is in the ignore list
                    if isinstance(value, bool): # map booleans to 0/1
                        values.append(int(value))
                    elif isinstance(value, (int, float)): # extract floats/ints
                        values.append(value)

            # Convert the list to a Numpy array
            return np.array(values)
        
        # convert the two dicts
        t1: np.ndarray = process_dict(track1)
        t2: np.ndarray = process_dict(track2)

        # Calculate Euclidean distance between the tracks
        return np.linalg.norm((t1-t2))

    def evaluate_metrics(self, threshold=0) -> None:
        """
        Method for evaluating a given metric threshold over a random batch of nodes.

        Args:
            threshold (float): The threshold to evaluate whether a relationship should be created .
        """
        if len(self.random_nodes)==0: # check if the database has been randomly sampled
            self.random_sample()
            print("randomly sampled")

        with self.driver.session() as session:
            # run for every node of the artist's tracks in question
            for node_artist in self.artists_nodes: 
                # create a dict of the track props
                node1_values:dict = session.run(f"MATCH (t:Track) WHERE ID(t) = {node_artist} RETURN t").single()['t']._properties
                for pair_node in tqdm(self.random_nodes):
                    # create a dictionary of the random props
                    node2_values:dict = session.run(f"MATCH (t:Track) WHERE ID(t) = {pair_node} RETURN t").single()['t']._properties

                    # eval the sim score
                    similarity_score: float = self.eucliean_distance(node1_values, node2_values)

                    # create a relationship if the sim score is above the threshold
                    if similarity_score > threshold:
                        self.create_relationship(node_artist, pair_node, "MATCHED", f"{{sim_score: {similarity_score}}}")

    def normalize_data(self) -> np.ndarray:
        """
        Normalizes the features of the tracks

        Returns:
            np.ndarray: The normalized features of the tracks.

        """
        # Get the top recommended songs
        with self.driver.session() as session:
            for key in self.track_keys:
                if key not in self.exclude_keys:
                    # normalize all the features
                    query: str = f"MATCH (t:Track) RETURN MAX(t.{key}) AS max_{key}, MIN(t.{key}) AS min_{key}"
                    result = session.run(query)
                    record = result.single()
                    self.max_min_values[key] = (record[f"max_{key}"], record[f"min_{key}"])

            # normalize all features for all random nodes
            print("break1")
            for ele in tqdm(self.random_nodes):
                track = session.run(f"MATCH (t:Track) WHERE ID(t) ={ele} RETURN t").single()
                for key in self.track_keys:
                    if key not in self.exclude_keys:
                        # Update the track feature in the database
                        track_feature = track['t']._properties[key]
                        try:
                            normalized_feature = (track_feature - self.max_min_values[key][1]) / (
                                        self.max_min_values[key][0] - self.max_min_values[key][1])
                            query = f"MATCH (t:Track) WHERE ID(t) = {ele} SET t.{key} = {normalized_feature}"
                            session.run(query)
                            session.commit()
                        except:
                            pass
            print("break2")
            for ele in tqdm(self.artists_nodes):
                track = session.run(f"MATCH (t:Track) WHERE ID(t) ={ele} RETURN t").single()
                for key in self.track_keys:
                    if key not in self.exclude_keys:
                        # Update the track feature in the database
                        track_feature = track['t']._properties[key]
                        try:
                            normalized_feature = (track_feature - self.max_min_values[key][1]) / (
                                        self.max_min_values[key][0] - self.max_min_values[key][1])
                            query = f"MATCH (t:Track) WHERE ID(t) = {ele} SET t.{key} = {normalized_feature}"
                            session.run(query)
                            session.commit()
                        except:
                            pass
            print("break4")
                        
    def random_sample(self, batch_size=1500, artist="Regina Spektor") -> None:
        """
        Randomly sampled the database with a given batch size for a given artist.

        Args:
            batch_size (int): The size of the batch to sample.
            artist (str): The artist to include in the sample.
        """
        with self.driver.session() as session:
            # query string for random songs
            query:str = f"MATCH (t:Track) WHERE NOT t.artist = '{artist}' WITH t, rand() AS r ORDER BY r RETURN ID(t) AS track_id LIMIT {batch_size}"
            result = session.run(query)
            # appends to the random node list the track IDs
            for record in result:
                self.random_nodes.append(record["track_id"])
            # query string for artist songs
            query_artist: str = f"MATCH (t:Track) WHERE t.artist = '{artist}' RETURN ID(t) AS track_id"
            res_art = session.run(query_artist)
            # append to the artist node lsit the track IDs
            for rec in res_art:
                self.artists_nodes.append(rec['track_id'])

    def find_recommended_songs(self, num_recommendations=5, artist="Regina Spektor") -> set:
        """
        Given an artist, finds recommended songs using to a certain number

        Args:
            num_recommendations (int): The number of recommendations to return.
            artist (str): The artist to find recommendations for.

        Returns:
            set: The set of recommended songs.
        """
        # initialized a list of random songs
        recommended_songs: list = []

        # Get the top recommended songs
        with self.driver.session() as session:
            query: str = f"MATCH (t1:Track)-[r]->(t2:Track) WHERE t1.artist = '{artist}' RETURN t2.id, t2.name, t2.artist ORDER BY r.sim_score ASC LIMIT {num_recommendations}"
            result = session.run(query)
            # append the name and artist to the song lsit
            for record in result:
                recommended_songs.append(f"{record['t2.name']}, {record['t2.artist']}")
        
        # return unique songs
        return set(recommended_songs)


if __name__ == "__main__":
    print("initialize driver")
    driving: Neo4jDriver = Neo4jDriver("neo4j://localhost:7687", "neo4j", "password")
    driving.connect()
    print("driver working")

    # drop existing database data
    driving.flush_database()
    print("Data flushed")

    # fill the db with spotify csv data
    driving.set_spotify_schema()
    print("Data added")

    # set randomly sampled tracks
    if len(driving.random_nodes) == 0:
        driving.random_sample(batch_size=1500)
        print("Sampling complete")


    # normalize the data
    driving.normalize_data()

    # evaluate similarity metrics
    driving.evaluate_metrics()

    # query the top reccomended songs and print
    songs: list = driving.find_recommended_songs()
    print(songs)

    driving.disconnect()
