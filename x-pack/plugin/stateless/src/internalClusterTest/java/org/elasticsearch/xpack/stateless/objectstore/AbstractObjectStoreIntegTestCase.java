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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodeResponse;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesRequest;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesResponse;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link ObjectStoreService} types.
 */
public abstract class AbstractObjectStoreIntegTestCase extends AbstractStatelessIntegTestCase {

    protected abstract String repositoryType();

    protected Settings repositorySettings() {
        return Settings.builder().put("compress", randomBoolean()).build();
    }

    public void testBlobCreateVerifyDelete() throws Exception {
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = objectStoreService.getObjectStore();

        logger.info("--> About to write test blob");
        BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
        BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
        blobContainer.writeBlob(operationPurpose, "test.txt", whatToWrite, true);
        logger.info("--> Wrote blob");

        assertEquals(1, blobContainer.listBlobs(operationPurpose).size());
        try (InputStream is = blobContainer.readBlob(operationPurpose, "test.txt")) {
            assertArrayEquals(whatToWrite.array(), is.readAllBytes());
        }

        logger.info("--> Deleting");
        blobContainer.delete(operationPurpose);
        assertEquals(0, blobContainer.listBlobs(operationPurpose).size());
    }

    public void testBlobStoreStats() throws IOException {
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = objectStoreService.getObjectStore();

        // randomly read, write and delete some data
        if (randomBoolean()) {
            BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
            BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
            blobContainer.writeBlob(operationPurpose, "test.txt", whatToWrite, true);
            try (InputStream is = blobContainer.readBlob(operationPurpose, "test.txt")) {
                is.readAllBytes();
            }
            blobContainer.delete(operationPurpose);
        }

        // Create a repository and perform some snapshot actions
        createRepository("backup", repositorySettings());
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot").get().isAcknowledged(), is(true));

        GetBlobStoreStatsNodesResponse getBlobStoreStatsNodesResponse = client().execute(
            Stateless.GET_BLOB_STORE_STATS_ACTION,
            new GetBlobStoreStatsNodesRequest()
        ).actionGet();

        final List<GetBlobStoreStatsNodeResponse> nodeResponses = getBlobStoreStatsNodesResponse.getNodes();

        assertEquals(1, nodeResponses.size());
        assertRepositoryStats(nodeResponses.get(0).getRepositoryStats());
        assertObsRepositoryStatsSnapshots(nodeResponses.get(0).getObsRepositoryStats());

        // Test result from talking to a specific node
        final GetBlobStoreStatsNodeResponse nodeResponse = client().execute(
            Stateless.GET_BLOB_STORE_STATS_ACTION,
            new GetBlobStoreStatsNodesRequest()
        ).actionGet().getNodes().get(0);
        assertRepositoryStats(nodeResponse.getRepositoryStats());
        assertObsRepositoryStatsSnapshots(nodeResponse.getObsRepositoryStats());

        // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
        internalCluster().stopCurrentMasterNode();
    }

    protected abstract void assertRepositoryStats(RepositoryStats repositoryStats);

    protected abstract void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats);

    protected void createRepository(String repoName, Settings repoSettings) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType(repositoryType())
                .setVerify(randomBoolean())
                .setSettings(repoSettings)
        );
    }
}
