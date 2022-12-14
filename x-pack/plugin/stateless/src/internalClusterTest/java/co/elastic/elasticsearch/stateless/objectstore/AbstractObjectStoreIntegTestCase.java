package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.InputStream;

/**
 * Integration tests for {@link ObjectStoreService} types.
 */
public abstract class AbstractObjectStoreIntegTestCase extends AbstractStatelessIntegTestCase {

    public void testBlobCreateVerifyDelete() throws Exception {
        startMasterAndIndexNode();
        var objectStoreService = internalCluster().getAnyMasterNodeInstance(ObjectStoreService.class);
        BlobStoreRepository repository = objectStoreService.getObjectStore();

        logger.info("--> About to write test blob");
        BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
        BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
        blobContainer.writeBlob("test.txt", whatToWrite, true);
        logger.info("--> Wrote blob");

        assertEquals(1, blobContainer.listBlobs().size());
        try (InputStream is = blobContainer.readBlob("test.txt")) {
            assertArrayEquals(whatToWrite.array(), is.readAllBytes());
        }

        logger.info("--> Deleting");
        blobContainer.delete();
        assertEquals(0, blobContainer.listBlobs().size());
    }

}
