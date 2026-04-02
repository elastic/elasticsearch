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

package org.elasticsearch.xpack.stateless.objectstore;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService} types.
 */
public abstract class AbstractObjectStoreIntegTestCase extends AbstractStatelessPluginIntegTestCase {

    private static StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus;

    @BeforeClass
    public static void initStatelessSnapshotEnabledStatus() {
        statelessSnapshotEnabledStatus = randomFrom(StatelessSnapshotEnabledStatus.values());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
            statelessSnapshotEnabledStatus
        );
    }

    protected abstract String repositoryType();

    protected Settings repositorySettings() {
        return Settings.builder().put("compress", randomBoolean()).build();
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    public void testBlobCreateVerifyDelete() throws Exception {
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = randomBoolean()
            ? objectStoreService.getClusterObjectStore()
            : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);

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
        assumeFalse("restore not yet working in multi-project mode", multiProjectIntegrationTest());
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = randomBoolean()
            ? objectStoreService.getClusterObjectStore()
            : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);

        // randomly read, write and delete some data
        var withRandomCrud = randomBoolean();
        if (withRandomCrud) {
            BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
            BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
            blobContainer.writeBlob(operationPurpose, "test.txt", whatToWrite, true);
            try (InputStream is = blobContainer.readBlob(operationPurpose, "test.txt")) {
                is.readAllBytes();
            }
            blobContainer.delete(operationPurpose);
        }

        final var indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        final int numDocs = between(1, 100);
        indexDocs(indexName, numDocs);

        // Create a repository and perform some snapshot actions
        createRepository("backup", repositorySettings());
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        safeGet(indicesAdmin().prepareDelete(indexName).execute());
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot").get().isAcknowledged(), is(true));

        assertThat(
            safeGet(indicesAdmin().prepareStats(indexName).execute()).getIndex(indexName).getTotal().getDocs().getCount(),
            equalTo((long) numDocs)
        );

        final List<RepositoryStats> repositoryStats = getRepositoryStats();

        assertEquals(1, repositoryStats.size());
        assertRepositoryStats(repositoryStats.get(0), withRandomCrud, operationPurpose);
        assertObsRepositoryStatsSnapshots(getObsRepositoryStats().get(0));
    }

    protected abstract void assertRepositoryStats(RepositoryStats repositoryStats, boolean withRandomCrud, OperationPurpose randomPurpose);

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
