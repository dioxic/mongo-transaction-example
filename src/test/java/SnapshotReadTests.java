import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotReadTests {

    private final MongoClient client = MongoClients.create();
    private final MongoCollection<Document> collection = client
            .getDatabase("test")
            .getCollection("collection");

    //4 inserts, the second and last element should fail due to a duplicate key exception
    private final List<WriteModel<Document>> writeModelsWithDuplicateKeys = List.of(
            new InsertOneModel<>(new Document("_id", 111)),
            new InsertOneModel<>(new Document("_id", 222)),
            new InsertOneModel<>(new Document("_id", 333)),
            new InsertOneModel<>(new Document("_id", 222))
    );

    private final List<WriteModel<Document>> writeModelsValid = List.of(
            new InsertOneModel<>(new Document("_id", 111)),
            new InsertOneModel<>(new Document("_id", 333))
    );

    @BeforeEach
    public void setup() {
        collection.drop();
    }

    @Test
    public void TestSnapshotSession() {
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder().snapshot(true).build();

        // insert some documents before starting the session
        collection.insertOne(new Document("_id", 111));

        ClientSession session = client.startSession(sessionOptions);

        //  the first read operation against the session sets the snapshot time
        //  all subsequent reads will read from the same snapshot
        collection.find(session).forEach(System.out::println);

        // after starting the snapshot session, we insert some records
        collection.insertOne(new Document("_id", 222));
        collection.insertOne(new Document("_id", 333));

        // reading using the snapshot session, I don't see document 222 or 333
        assertEquals(1, collection.countDocuments(session), "document count");

        // reading outside of the snapshot session, I can see documents 222 and 333
        assertEquals(3, collection.countDocuments(), "document count");

    }

}
