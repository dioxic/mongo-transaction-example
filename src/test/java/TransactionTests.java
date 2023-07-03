import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
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

    private final TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .writeConcern(WriteConcern.MAJORITY)
            .build();

    @BeforeEach
    public void setup() {
        collection.drop();
        collection.insertOne(new Document("_id", 222));
    }

    /**
     * Showing unordered bulk write outside of a transaction
     */
    @Test
    public void DuplicateKey() {
        MongoBulkWriteException ex = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> collection.bulkWrite(writeModelsWithDuplicateKeys, options));

        // when ordered = false we expect 2 inserts and 2 errors
        assertEquals(2, ex.getWriteErrors().size());
        assertEquals(2, ex.getWriteResult().getInsertedCount());

    }

    /**
     * Replicating the error with duplicate key exception in a transaction
     */
    @Test
    public void DuplicateKeyInTransaction() {
        ClientSession session = client.startSession();

        // try and do a bulk write with duplicate documents
        MongoBulkWriteException mbwe = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> {
                    session.startTransaction(txnOptions);
                    collection.bulkWrite(session, writeModelsWithDuplicateKeys, options);
                    session.commitTransaction();
                });

        System.out.println(mbwe.getMessage());

        // in a transaction, bulk writes will fast-fail so we only get a single write error and a single insert
        assertEquals(1, mbwe.getWriteErrors().size(), "write errors");
        assertEquals(1, mbwe.getWriteResult().getInsertedCount(), "inserts");

        // this is true even though the transaction has aborted because the activeTransaction state
        // needs to be manually managed if you don't use the withTransaction method
        assertTrue(session.hasActiveTransaction(), "activeTransaction");

        // we retry the bulk write with the problem writemodels removed
        MongoCommandException mce = assertThrowsExactly(
                MongoCommandException.class,
                () -> {
                    collection.bulkWrite(session, writeModelsValid, options);
                    session.commitTransaction();
                });

        // we get a no transaction error from the server
        assertEquals("NoSuchTransaction", mce.getErrorCodeName());

        System.out.println(mce.getMessage());
    }

    /**
     * Replicating the error with duplicate key exception in a transaction
     * but this time using the [withTransaction] method.
     */
    @Test
    public void DuplicateKeyWithTransactionBody() {
        ClientSession session = client.startSession();

        // creating a transaction body lambda
        TransactionBody<BulkWriteResult> txnBodyFail = () ->
                collection.bulkWrite(session, writeModelsWithDuplicateKeys, options);

        // executing the transaction body and catching the exception
        MongoBulkWriteException mbwe = assertThrowsExactly(
                MongoBulkWriteException.class,
                () -> session.withTransaction(txnBodyFail, txnOptions)
        );

        // this is now false - when using the withTransaction method, the transaction status is correct
        assertFalse(session.hasActiveTransaction(), "activeTransaction");

        // creating a new transaction body with valid write models
        TransactionBody<BulkWriteResult> txnBodyValid = () ->
            collection.bulkWrite(session, writeModelsValid, options);

        // no errors thrown and we get the reuslts
        BulkWriteResult results = session.withTransaction(txnBodyValid, txnOptions);

        assertEquals(2, results.getInsertedCount(), "inserts");

    }

}
