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

public class TransactionTests {

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

    private final BulkWriteOptions options = new BulkWriteOptions().ordered(false);

    @BeforeEach
    public void setup() {
        collection.drop();
        collection.insertOne(new Document("_id", 222));
    }

    @Test
    public void TestDuplicateKey() {
        MongoBulkWriteException ex = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> collection.bulkWrite(writeModelsWithDuplicateKeys, options));

        // when ordered = false we expect 2 inserts and 2 errors
        assertEquals(2, ex.getWriteErrors().size());
        assertEquals(2, ex.getWriteResult().getInsertedCount());

    }

    @Test
    public void TestDuplicateKeyInTransaction() {
        ClientSession session = client.startSession();

        MongoBulkWriteException mbwe = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> {
                    session.startTransaction();
                    collection.bulkWrite(session, writeModelsWithDuplicateKeys, options);
                    session.commitTransaction();
                });

        System.out.println(mbwe.getMessage());

        // in a transaction, bulk writes will fast-fail so we only get a single write error and a single insert
        assertEquals(1, mbwe.getWriteErrors().size(), "write errors");
        assertEquals(1, mbwe.getWriteResult().getInsertedCount(), "inserts");

        // this is true even though the transaction has aborted
        assertTrue(session.hasActiveTransaction(), "activeTransaction");

        // we retry the bulk write with the problem writemodels removed
        MongoCommandException mce = assertThrowsExactly(
                MongoCommandException.class,
                () -> {
                    collection.bulkWrite(session, writeModelsValid, options);
                    session.commitTransaction();
                });

        // there is a no transaction error from the server
        assertEquals("NoSuchTransaction", mce.getErrorCodeName());

        System.out.println(mce.getMessage());

    }

    @Test
    public void TestDuplicateKeyWithTransactionBody() {
        ClientSession session = client.startSession();

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        TransactionBody<String> txnBodyFail = () -> {
            collection.bulkWrite(session, writeModelsWithDuplicateKeys, options);
            return "Inserted into collections in different databases";
        };

        MongoBulkWriteException mbwe = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> session.withTransaction(txnBodyFail, txnOptions)
        );

        // this should be false - when using the withTransaction method, the transaction status is correct
        assertFalse(session.hasActiveTransaction(), "activeTransaction");

        // in a transaction, bulk writes will fast-fail so we only get a single write error and a single insert
        assertEquals(1, mbwe.getWriteErrors().size(), "write errors");
        assertEquals(1, mbwe.getWriteResult().getInsertedCount(), "inserts");
        System.out.println(mbwe.getMessage());

        TransactionBody<String> txnBodyValid = () -> {
            collection.bulkWrite(session, writeModelsValid, options);
            return "Inserted into collections in different databases";
        };

        session.withTransaction(txnBodyValid, txnOptions);

        // if the transaction succeeds, there should be 3 document (1 from test setup, 2 from the bulk write)
        assertEquals(3, collection.countDocuments());

    }

}
