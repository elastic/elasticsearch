/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsAction;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodeResponse;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesRequest;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesResponse;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

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

    public void testBlobStoreStats() throws IOException {
        startMasterAndIndexNode();
        var objectStoreService = internalCluster().getAnyMasterNodeInstance(ObjectStoreService.class);
        BlobStoreRepository repository = objectStoreService.getObjectStore();

        // randomly read, write and delete some data
        if (randomBoolean()) {
            BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
            BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
            blobContainer.writeBlob("test.txt", whatToWrite, true);
            try (InputStream is = blobContainer.readBlob("test.txt")) {
                is.readAllBytes();
            }
            blobContainer.delete();
        }

        GetBlobStoreStatsNodesResponse getBlobStoreStatsNodesResponse = client().execute(
            GetBlobStoreStatsAction.INSTANCE,
            new GetBlobStoreStatsNodesRequest()
        ).actionGet();

        final List<GetBlobStoreStatsNodeResponse> nodeResponses = getBlobStoreStatsNodesResponse.getNodes();

        assertEquals(1, nodeResponses.size());
        assertRepositoryStats(nodeResponses.get(0).getRepositoryStats());

        // Result is the same when the request asks for the specific node
        assertThat(
            client().execute(
                GetBlobStoreStatsAction.INSTANCE,
                new GetBlobStoreStatsNodesRequest(getBlobStoreStatsNodesResponse.getNodes().get(0).getNode().getId())
            ).actionGet(),
            equalTo(getBlobStoreStatsNodesResponse)
        );
    }

    protected abstract void assertRepositoryStats(RepositoryStats repositoryStats);
}
