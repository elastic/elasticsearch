/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class BlobStoreMetadataIntegrityIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    private Releasable integrityCheckSuppressor;

    @Before
    public void suppressIntegrityChecks() {
        disableRepoConsistencyCheck("testing integrity checks involves breaking the repo");
        assertNull(integrityCheckSuppressor);
        integrityCheckSuppressor = new BlobStoreIndexShardSnapshotsIntegritySuppressor();
    }

    @After
    public void enableIntegrityChecks() {
        Releasables.closeExpectNoException(integrityCheckSuppressor);
        integrityCheckSuppressor = null;
    }

    @TestLogging(reason = "testing", value = "org.elasticsearch.repositories.blobstore.MetadataVerifier:DEBUG")
    public void testIntegrityCheck() throws Exception {
        final var repoPath = randomRepoPath();
        createRepository(
            REPOSITORY_NAME,
            "mock",
            Settings.builder().put(BlobStoreRepository.SUPPORT_URL_REPO.getKey(), false).put("location", repoPath)
        );
        final MockRepository repository = (MockRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(REPOSITORY_NAME);

        final var indexCount = between(1, 3);
        for (int i = 0; i < indexCount; i++) {
            createIndexWithRandomDocs("test-index-" + i, between(1, 1000));
        }

        final var snapshotCount = between(2, 4);
        for (int snapshotIndex = 0; snapshotIndex < snapshotCount; snapshotIndex++) {
            final var indexRequests = new ArrayList<IndexRequestBuilder>();
            for (int i = 0; i < indexCount; i++) {
                if (randomBoolean()) {
                    final var indexName = "test-index-" + i;
                    if (randomBoolean()) {
                        assertAcked(client().admin().indices().prepareDelete(indexName));
                        createIndexWithRandomDocs(indexName, between(1, 1000));
                    }
                    final var numDocs = between(1, 1000);
                    for (int doc = 0; doc < numDocs; doc++) {
                        indexRequests.add(client().prepareIndex(indexName).setSource("field1", "bar " + doc));
                    }
                }
            }
            indexRandom(true, indexRequests);
            assertEquals(0, client().admin().indices().prepareFlush().get().getFailedShards());
            final var snapshotInfo = clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, "test-snapshot-" + snapshotIndex)
                .setIncludeGlobalState(randomBoolean())
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo();
            assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
            assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        }

        repository.setBlockOnReadIndexMeta();

        final var tasksFuture = new PlainActionFuture<List<TaskInfo>>();
        repository.threadPool().generic().execute(() -> {
            try {
                assertBusy(() -> assertTrue(repository.blocked()));
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            ActionListener.completeWith(
                tasksFuture,
                () -> client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().equals(VerifyRepositoryIntegrityAction.NAME) && t.status() != null)
                    .toList()
            );

            repository.unblock();
        });

        verifyAndAssertSuccessful(indexCount);

        final var tasks = tasksFuture.actionGet(30, TimeUnit.SECONDS);
        assertThat(tasks, not(empty()));
        for (TaskInfo task : tasks) {
            if (task.status()instanceof VerifyRepositoryIntegrityAction.Status status) {
                assertEquals(REPOSITORY_NAME, status.repositoryName());
                assertThat(status.repositoryGeneration(), greaterThan(0L));
                assertEquals(snapshotCount, status.snapshotCount());
                assertEquals(snapshotCount, status.snapshotsVerified());
                assertEquals(indexCount, status.indexCount());
                assertEquals(0, status.indicesVerified());
                assertThat(status.indexSnapshotCount(), greaterThanOrEqualTo((long) indexCount));
                assertEquals(0, status.indexSnapshotsVerified());
                assertEquals(0, status.anomalyCount());
            } else {
                assert false : Strings.toString(task);
            }
        }

        final var tempDir = createTempDir();

        final var repositoryData = PlainActionFuture.get(repository::getRepositoryData, 10, TimeUnit.SECONDS);
        final var repositoryDataBlob = repoPath.resolve("index-" + repositoryData.getGenId());

        final List<Path> blobs;
        try (var paths = Files.walk(repoPath)) {
            blobs = paths.filter(path -> Files.isRegularFile(path) && path.equals(repositoryDataBlob) == false).sorted().toList();
        }

        for (int i = 0; i < 2000; i++) {
            final var blobToDamage = randomFrom(blobs);
            final var isDataBlob = blobToDamage.getFileName().toString().startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);
            final var truncate = randomBoolean();
            if (truncate) {
                logger.info("--> truncating {}", blobToDamage);
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                Files.write(blobToDamage, new byte[0]);
            } else if (isDataBlob || randomBoolean()) {
                logger.info("--> deleting {}", blobToDamage);
                Files.move(blobToDamage, tempDir.resolve("tmp"));
            } else {
                logger.info("--> corrupting {}", blobToDamage);
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                CorruptionUtils.corruptFile(random(), blobToDamage);
            }
            try {
                // TODO include some cancellation tests

                final var anomalies = verifyAndGetAnomalies(indexCount, repoPath.relativize(blobToDamage));
                if (isDataBlob) {
                    final var expectedAnomaly = truncate
                        ? MetadataVerifier.Anomaly.MISMATCHED_BLOB_LENGTH
                        : MetadataVerifier.Anomaly.MISSING_BLOB;
                    var foundExpectedAnomaly = false;
                    for (SearchHit anomaly : anomalies) {
                        final var source = anomaly.getSourceAsMap();
                        if (expectedAnomaly.toString().equals(source.get("anomaly"))
                            && blobToDamage.getFileName().toString().equals(source.get("blob_name"))) {
                            foundExpectedAnomaly = true;
                            break;
                        }
                    }
                    assertTrue(foundExpectedAnomaly);
                }

                //
                // final var isCancelled = new AtomicBoolean();
                //
                // final var verificationResponse = PlainActionFuture.get(
                // (PlainActionFuture<Void> listener) -> repository.verifyMetadataIntegrity(
                // client(),
                // () -> new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE),
                // request,
                // listener,
                // () -> {
                // if (rarely() && rarely()) {
                // isCancelled.set(true);
                // return true;
                // }
                // return isCancelled.get();
                // }
                // ),
                // 30,
                // TimeUnit.SECONDS
                // );
                // for (SearchHit hit : client().prepareSearch("metadata_verification_results").setSize(10000).get().getHits().getHits()) {
                // logger.info("--> {}", Strings.toString(hit));
                // }
                // assertThat(verificationResponse, not(nullValue()));
                // final var responseString = verificationResponse.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"));
                // if (isCancelled.get()) {
                // assertThat(responseString, containsString("verification task cancelled before completion"));
                // }
                // if (isDataBlob && isCancelled.get() == false) {
                // assertThat(
                // responseString,
                // allOf(containsString(blobToDamage.getFileName().toString()), containsString("missing blob"))
                // );
                // }
            } finally {
                Files.deleteIfExists(blobToDamage);
                Files.move(tempDir.resolve("tmp"), blobToDamage);
            }

            verifyAndAssertSuccessful(indexCount);
        }
    }

    private void verifyAndAssertSuccessful(int indexCount) {
        PlainActionFuture.<ActionResponse.Empty, RuntimeException>get(
            listener -> client().execute(
                VerifyRepositoryIntegrityAction.INSTANCE,
                new VerifyRepositoryIntegrityAction.Request(REPOSITORY_NAME, Strings.EMPTY_ARRAY, 0, 0, 0, 0),
                listener
            ),
            30,
            TimeUnit.SECONDS
        );
        assertEquals(
            0,
            client().prepareSearch("metadata_verification_results")
                .setSize(0)
                .setQuery(new ExistsQueryBuilder("anomaly"))
                .get()
                .getHits()
                .getTotalHits().value
        );
        assertEquals(
            indexCount,
            client().prepareSearch("metadata_verification_results")
                .setSize(0)
                .setQuery(new ExistsQueryBuilder("restorability"))
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value
        );
        assertEquals(
            indexCount,
            client().prepareSearch("metadata_verification_results")
                .setSize(0)
                .setQuery(new TermQueryBuilder("restorability", "full"))
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value
        );
        assertEquals(
            0,
            client().prepareSearch("metadata_verification_results")
                .setSize(1)
                .setQuery(new TermQueryBuilder("completed", true))
                .get()
                .getHits()
                .getHits()[0].getSourceAsMap().get("total_anomalies")
        );
        assertAcked(client().admin().indices().prepareDelete("metadata_verification_results"));
    }

    private SearchHit[] verifyAndGetAnomalies(long indexCount, Path damagedBlob) {
        PlainActionFuture.<ActionResponse.Empty, RuntimeException>get(
            listener -> client().execute(
                VerifyRepositoryIntegrityAction.INSTANCE,
                new VerifyRepositoryIntegrityAction.Request(REPOSITORY_NAME, Strings.EMPTY_ARRAY, 0, 0, 0, 0),
                listener
            ),
            30,
            TimeUnit.SECONDS
        );
        final var anomalyHits = client().prepareSearch("metadata_verification_results")
            .setSize(10000)
            .setQuery(new ExistsQueryBuilder("anomaly"))
            .get()
            .getHits();
        assertThat(anomalyHits.getTotalHits().value, greaterThan(0L));
        assertEquals(
            indexCount,
            client().prepareSearch("metadata_verification_results")
                .setSize(0)
                .setQuery(new ExistsQueryBuilder("restorability"))
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value
        );
        final var damagedFileName = damagedBlob.getFileName().toString();
        assertThat(
            client().prepareSearch("metadata_verification_results")
                .setSize(0)
                .setQuery(new TermQueryBuilder("restorability", "full"))
                .setTrackTotalHits(true)
                .get()
                .getHits()
                .getTotalHits().value,
            damagedFileName.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)
                || damagedFileName.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)
                || (damagedFileName.startsWith(BlobStoreRepository.METADATA_PREFIX) && damagedBlob.startsWith("indices"))
                    ? lessThan(indexCount)
                    : equalTo(indexCount)
        );
        final int totalAnomalies = (int) client().prepareSearch("metadata_verification_results")
            .setSize(1)
            .setQuery(new TermQueryBuilder("completed", true))
            .get()
            .getHits()
            .getHits()[0].getSourceAsMap().get("total_anomalies");
        assertThat(totalAnomalies, greaterThan(0));
        assertAcked(client().admin().indices().prepareDelete("metadata_verification_results"));
        return anomalyHits.getHits();
    }
}
