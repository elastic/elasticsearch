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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class StatelessCommitServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private int primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(StatelessCommitServiceTests.class.getName(), Settings.EMPTY);
        primaryTerm = randomIntBetween(1, 100);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCommitUpload() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(3, 8));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                var generationalFiles = commitRef.getCommitFiles().stream().filter(StatelessCommitService::isGenerationalFile).toList();
                var internalFiles = returnInternalFiles(testHarness, List.of(blobNameFromGeneration(commitRef.getGeneration())));
                assertThat(generationalFiles, everyItem(is(in(internalFiles))));
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testCommitUploadIncludesRetries() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> failedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());

        try (var testHarness = createNode((fileName, runnable) -> {
            if (failedFiles.contains(fileName) == false && randomBoolean()) {
                failedFiles.add(fileName);
                throw new IOException("Failed");
            } else {
                // Rarely fail a second time
                if (failedFiles.contains(fileName) && rarely()) {
                    throw new IOException("Failed");
                } else {
                    runnable.run();
                    uploadedBlobs.add(fileName);
                }
            }

        }, fileCapture(uploadedBlobs))) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(2, 4));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testSecondCommitDefersSchedulingForFirstCommit() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicReference<String> commitFileToBlock = new AtomicReference<>();
        AtomicReference<String> firstCommitFile = new AtomicReference<>();
        AtomicReference<String> secondCommitFile = new AtomicReference<>();

        CountDownLatch startingUpload = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileCapture = fileCapture(uploadedBlobs);
        try (var testHarness = createNode(fileCapture(uploadedBlobs), (compoundCommitFile, runnable) -> {
            if (compoundCommitFile.equals(firstCommitFile.get())) {
                try {
                    startingUpload.countDown();
                    blocking.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertFalse(uploadedBlobs.contains(secondCommitFile.get()));
            } else {
                assertEquals(compoundCommitFile, secondCommitFile.get());
                assertTrue(uploadedBlobs.contains(firstCommitFile.get()));
            }
            compoundCommitFileCapture.accept(compoundCommitFile, runnable);
        })) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, 2);
            StatelessCommitRef firstCommit = commitRefs.get(0);
            StatelessCommitRef secondCommit = commitRefs.get(1);

            commitFileToBlock.set(
                firstCommit.getAdditionalFiles()
                    .stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .findFirst()
                    .get()
            );

            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();
            firstCommitFile.set(compoundCommitFiles.get(0));
            secondCommitFile.set(compoundCommitFiles.get(1));

            testHarness.commitService.onCommitCreation(firstCommit);
            startingUpload.await();
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            blocking.countDown();

            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testUploadFileOfUnregisteredShardThrowsExceptions() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            List<StatelessCommitRef> refs = generateIndexCommits(testHarness, 1);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, () -> testHarness.commitService.onCommitCreation(refs.get(0)));
        }
    }

    public void testMapIsPrunedOnIndexDelete() throws Exception {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            List<StatelessCommitRef> refs = generateIndexCommits(testHarness, 2, true);
            StatelessCommitRef firstCommit = refs.get(0);
            StatelessCommitRef secondCommit = refs.get(1);

            testHarness.commitService.onCommitCreation(firstCommit);

            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future);
            future.actionGet();

            testHarness.commitService.onCommitCreation(secondCommit);

            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future2);
            future2.actionGet();

            Set<String> files = testHarness.commitService.getFilesWithBlobLocations(testHarness.shardId);

            assertThat(
                "Expected that all first commit files " + firstCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(firstCommit.getCommitFiles().toArray(String[]::new))
            );
            assertThat(
                "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
            );

            testHarness.commitService.markCommitDeleted(testHarness.shardId, firstCommit.getGeneration());
            testHarness.commitService.closedLocalReadersForGeneration(testHarness.shardId).accept(firstCommit.getGeneration());

            List<String> expectedDeletedFiles = firstCommit.getCommitFiles()
                .stream()
                .filter(f -> secondCommit.getCommitFiles().contains(f) == false)
                .toList();

            // It might take a while until the reference held to send the new commit notification is released
            assertBusy(() -> {
                Set<String> filesAfterDelete = testHarness.commitService.getFilesWithBlobLocations(testHarness.shardId);

                assertThat(
                    "Expected that all first commit only files "
                        + expectedDeletedFiles
                        + " to be deleted from file map "
                        + filesAfterDelete,
                    filesAfterDelete,
                    not(hasItems(expectedDeletedFiles.toArray(String[]::new)))
                );
                assertThat(
                    "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                    files,
                    hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
                );
            });
        }
    }

    public void testRecoveredCommitIsNotUploadedAgain() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            StatelessCommitRef commitRef = generateIndexCommits(testHarness, 1).get(0);

            List<String> commitFiles = commitRef.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
            future.actionGet();

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, List.of(blobNameFromGeneration(commitRef.getGeneration()))));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that the compound commit file "
                    + blobNameFromGeneration(commitRef.getGeneration())
                    + " has been uploaded "
                    + uploadedFiles,
                uploadedFiles,
                hasItems(blobNameFromGeneration(commitRef.getGeneration()))
            );

            uploadedBlobs.clear();

            Tuple<StatelessCompoundCommit, Set<BlobFile>> indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future2);
            future2.actionGet();

            assertThat(uploadedBlobs, empty());
        }
    }

    public void testWaitForGenerationFailsForClosedShard() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            ShardId unregisteredShardId = new ShardId("index", "uuid", 0);
            expectThrows(
                AlreadyClosedException.class,
                () -> testHarness.commitService.addListenerForUploadedGeneration(unregisteredShardId, 1, PlainActionFuture.newFuture())
            );
            PlainActionFuture<Void> failedFuture = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, 1, failedFuture);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
        }
    }

    public void testCommitsTrackingTakesIntoAccountSearchNodeUsage() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();
                pendingNotification.respondWithUsedCommits(new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()));
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedCommitPendingNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            mergedCommitPendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOldCommitsAreRetainedIfSearchNodesUseThem() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();

                pendingNotification.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, firstCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedCommitPendingNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            // Commits 0-9 are reported to be used by the search node;
            // therefore we should retain them even if the indexing node has deleted them
            assertThat(deletedCommits, empty());

            // Now the search shard, report that it uses commit 0 and commit 10; therefore we should delete commits [1, 8]
            mergedCommitPendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(primaryTerm, firstCommit.getGeneration()),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .filter(commit -> commit.primaryTermAndGeneration().generation() != firstCommit.getGeneration())
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOldCommitsAreRetainedIfIndexShardUseThem() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            for (StatelessCommitRef indexCommit : initialCommits) {
                commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            }

            // Commits 0-9 are still used by the indexing shard
            // therefore we should retain them even if the indexing node/lucene has deleted them
            assertThat(deletedCommits, empty());

            // Now the index shard, report that it no longer uses commits [1;9] and they should be deleted
            LongConsumer closedLocalReadersForGeneration = commitService.closedLocalReadersForGeneration(shardId);
            initialCommits.stream()
                .filter(c -> c.getGeneration() != firstCommit.getGeneration())
                .forEach(c -> closedLocalReadersForGeneration.accept(c.getGeneration()));

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .filter(commit -> commit.primaryTermAndGeneration().generation() != firstCommit.getGeneration())
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testRetainedCommitsAreReleasedAfterANodeIsUnassigned() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();
                pendingNotification.respondWithUsedCommits(new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()));
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedCommitPendingNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search node is still using commit 9 and the merged commit; therefore we should keep all commits around
            mergedCommitPendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(
                    initialCommits.get(initialCommits.size() - 1).getPrimaryTerm(),
                    initialCommits.get(initialCommits.size() - 1).getGeneration()
                ),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            assertThat(deletedCommits, empty());

            var unassignedSearchShard = TestShardRouting.newShardRouting(
                shardId,
                null,
                null,
                false,
                ShardRoutingState.UNASSIGNED,
                ShardRouting.Role.SEARCH_ONLY
            );

            var clusterStateWithUnassignedSearchShard = ClusterState.builder(state)
                .routingTable(
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(shardId.getIndex())
                                .addShard(state.routingTable().shardRoutingTable(shardId).primaryShard())
                                .addShard(unassignedSearchShard)
                        )
                        .build()
                )
                .build();

            commitService.clusterChanged(new ClusterChangedEvent("unassigned search shard", clusterStateWithUnassignedSearchShard, state));

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOutOfOrderNewCommitNotifications() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 2);
            var pendingNotificationResponses = new ArrayList<PendingNewCommitNotification>();
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();
                pendingNotificationResponses.add(pendingNotification);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedCommitPendingNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search shard is using the latest commit, but there are concurrent in-flight notifications
            mergedCommitPendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            if (randomBoolean()) {
                commitService.markCommitDeleted(shardId, mergedCommit.getGeneration());
            }

            for (PendingNewCommitNotification pendingNewCommitNotification : pendingNotificationResponses) {
                pendingNewCommitNotification.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, pendingNewCommitNotification.request().getGeneration())
                );
            }

            // There were multiple in-flight new commit notification responses, since we don't know what's the correct ordering, we should
            // consider the union of all responses in order to avoid deleting commits that are still used. In this case we cannot delete
            // anything since all commits are supposedly used.
            assertThat(deletedCommits, empty());
        }
    }

    public void testCommitsAreReleasedImmediatelyAfterDeletionWhenThereAreZeroReplicas() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(getTestName()) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
                    }
                };
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 2);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            PlainActionFuture<Void> mergedCommitUploadedFuture = new PlainActionFuture<>();
            commitService.addListenerForUploadedGeneration(shardId, mergedCommit.getGeneration(), mergedCommitUploadedFuture);
            mergedCommitUploadedFuture.actionGet();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .collect(Collectors.toSet());
            assertThat(deletedCommits, is(equalTo(expectedDeletedCommits)));
        }
    }

    public void testCommitsAreSuccessfullyInitializedAfterRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();
                pendingNotification.respondWithUsedCommits(new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()));
            }

            Tuple<StatelessCompoundCommit, Set<BlobFile>> indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedCommitPendingNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();

            StatelessCommitRef recoveryCommit = initialCommits.get(initialCommits.size() - 1);
            assert recoveryCommit.getGeneration() == indexingShardState.v1().generation();
            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.closedLocalReadersForGeneration(shardId).accept(recoveryCommit.getGeneration());

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            mergedCommitPendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = initialCommits.stream()
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testUnreferencedCommitsAreReleasedAfterRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(initialCommit.getGeneration()).get();
                pendingNotification.respondWithUsedCommits(new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()));
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            var mergedNotification = fakeSearchNode.getListenerForNewCommitNotification(mergedCommit.getGeneration()).get();
            mergedNotification.respondWithUsedCommits(new PrimaryTermAndGeneration(primaryTerm, mergedCommit.getGeneration()));

            testHarness.commitService.unregister(testHarness.shardId);

            Tuple<StatelessCompoundCommit, Set<BlobFile>> indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            StatelessCommitRef recoveryCommit = mergedCommit;
            assert recoveryCommit.getGeneration() == indexingShardState.v1().generation();

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            StatelessCommitRef newCommit = generateIndexCommits(testHarness, 1, true).get(0);
            StatelessCommitRef retainedBySearch = initialCommits.get(1);
            commitService.onCommitCreation(newCommit);
            var pendingNotification = fakeSearchNode.getListenerForNewCommitNotification(newCommit.getGeneration()).get();
            pendingNotification.respondWithUsedCommits(
                new PrimaryTermAndGeneration(primaryTerm, newCommit.getGeneration()),
                new PrimaryTermAndGeneration(primaryTerm, retainedBySearch.getGeneration())
            );

            // Retaining the second commit by the search node will retain the second AND first commit
            var expectedDeletedCommits = initialCommits.stream()
                .filter(
                    ref -> ref.getGeneration() != retainedBySearch.getGeneration()
                        && ref.getGeneration() != retainedBySearch.getGeneration() - 1
                )
                .map(
                    commit -> new StaleCompoundCommit(
                        shardId,
                        new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()),
                        primaryTerm
                    )
                )
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));

            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.closedLocalReadersForGeneration(shardId).accept(recoveryCommit.getGeneration());
            HashSet<StaleCompoundCommit> newDeleted = new HashSet<>(expectedDeletedCommits);
            newDeleted.add(
                new StaleCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, recoveryCommit.getGeneration()), primaryTerm)
            );
            assertThat(deletedCommits, equalTo(newDeleted));
        }
    }

    private record PendingNewCommitNotification(
        NewCommitNotificationRequest request,
        ActionListener<NewCommitNotificationResponse> listener
    ) {
        void respondWithUsedCommits(PrimaryTermAndGeneration... usedCommits) {
            listener.onResponse(new NewCommitNotificationResponse(Arrays.stream(usedCommits).collect(Collectors.toSet())));
        }
    }

    private static class FakeSearchNode extends NoOpNodeClient {
        private final Map<Long, List<ActionListener<PendingNewCommitNotification>>> generationPendingListeners = new HashMap<>();

        FakeSearchNode(String testName) {
            super(testName);
        }

        synchronized Future<PendingNewCommitNotification> getListenerForNewCommitNotification(long generation) {
            PlainActionFuture<PendingNewCommitNotification> future = new PlainActionFuture<>();
            generationPendingListeners.computeIfAbsent(generation, unused -> new ArrayList<>()).add(future);
            return future;
        }

        synchronized void onNewNotification(NewCommitNotificationRequest request, ActionListener<NewCommitNotificationResponse> listener) {
            List<ActionListener<PendingNewCommitNotification>> pendingListeners = generationPendingListeners.remove(
                request.getGeneration()
            );
            if (pendingListeners != null) {
                for (var generationListener : pendingListeners) {
                    generationListener.onResponse(new PendingNewCommitNotification(request, listener));
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            onNewNotification((NewCommitNotificationRequest) request, (ActionListener<NewCommitNotificationResponse>) listener);
        }
    }

    private ClusterState clusterStateWithPrimaryAndSearchShards(ShardId shardId, int searchNodes) {
        var indexNode = DiscoveryNodeUtils.create(
            "index_node",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)
        );

        var primaryShard = TestShardRouting.newShardRouting(shardId, indexNode.getId(), true, ShardRoutingState.STARTED);

        var discoveryNodes = DiscoveryNodes.builder().add(indexNode).localNodeId(indexNode.getId()).masterNodeId(indexNode.getId());

        var indexRoutingTable = IndexRoutingTable.builder(shardId.getIndex()).addShard(primaryShard);

        for (int i = 0; i < searchNodes; i++) {
            var searchNode = DiscoveryNodeUtils.create(
                "search_node" + i,
                buildNewFakeTransportAddress(),
                Map.of(),
                Set.of(DiscoveryNodeRole.SEARCH_ROLE)
            );
            discoveryNodes.add(searchNode);

            indexRoutingTable.addShard(
                TestShardRouting.newShardRouting(
                    shardId,
                    searchNode.getId(),
                    null,
                    false,
                    ShardRoutingState.STARTED,
                    ShardRouting.Role.SEARCH_ONLY
                )
            );
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes.build())
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(shardId.getIndex().getName())
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            )
                    )
                    .build()
            )
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    private ArrayList<String> returnInternalFiles(FakeStatelessNode testHarness, List<String> compoundCommitFiles) throws IOException {
        ArrayList<String> filesOnObjectStore = new ArrayList<>();
        for (String commitFile : compoundCommitFiles) {
            BlobContainer blobContainer = testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm);
            try (InputStream inputStream = blobContainer.readBlob(commitFile)) {
                long fileLength = blobContainer.listBlobs().get(commitFile).length();
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(
                    new InputStreamStreamInput(inputStream),
                    fileLength
                );
                compoundCommit.commitFiles()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue().blobName().equals(commitFile))
                    .forEach(e -> filesOnObjectStore.add(e.getKey()));

            }
        }
        return filesOnObjectStore;
    }

    private static CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> fileCapture(Set<String> uploadedFiles) {
        return (s, r) -> {
            r.run();
            uploadedFiles.add(s);
        };
    }

    private FakeStatelessNode createNode(
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> commitFileConsumer,
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileConsumer
    ) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                        throws IOException {
                        assertFalse(blobName, blobName.startsWith(IndexFileNames.SEGMENTS));
                        commitFileConsumer.accept(blobName, () -> super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists));
                    }

                    @Override
                    public void writeMetadataBlob(
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        assertTrue(blobName, StatelessCompoundCommit.startsWithBlobPrefix(blobName));
                        compoundCommitFileConsumer.accept(
                            blobName,
                            () -> super.writeMetadataBlob(blobName, failIfAlreadyExists, atomic, writer)
                        );
                    }

                    @Override
                    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
                        throw new AssertionError("should not be called");
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        };
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber) throws IOException {
        return generateIndexCommits(testHarness, commitsNumber, false);
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber, boolean merge)
        throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit = new HashSet<>();

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        String deleteId = randomAlphaOfLength(10);

        try (var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {
            for (int i = 0; i < commitsNumber; i++) {
                LuceneDocument document = new LuceneDocument();
                document.add(new KeywordField("field0", "term", Field.Store.YES));
                indexWriter.addDocument(document.getFields());
                if (randomBoolean()) {
                    final ParsedDocument tombstone = ParsedDocument.deleteTombstone(deleteId);
                    LuceneDocument delete = tombstone.docs().get(0);
                    NumericDocValuesField field = Lucene.newSoftDeletesField();
                    delete.add(field);
                    indexWriter.softUpdateDocument(EngineTestCase.newUid(deleteId), delete.getFields(), Lucene.newSoftDeletesField());
                }
                indexWriter.commit();
                if (merge) {
                    indexWriter.forceMerge(1, true);
                }
                try (var indexReader = DirectoryReader.open(indexWriter)) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    Set<String> commitFiles = new HashSet<>(indexCommit.getFileNames());
                    Set<String> additionalFiles = Sets.difference(commitFiles, previousCommit);
                    previousCommit = commitFiles;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        testHarness.shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> {}),
                        commitFiles,
                        additionalFiles,
                        primaryTerm,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }

    private static void markDeletedAndLocalUnused(List<StatelessCommitRef> commits, StatelessCommitService commitService, ShardId shardId) {
        LongConsumer closedLocalReadersForGeneration = commitService.closedLocalReadersForGeneration(shardId);
        for (StatelessCommitRef indexCommit : commits) {
            boolean deleteFirst = randomBoolean();
            if (deleteFirst == false) {
                closedLocalReadersForGeneration.accept(indexCommit.getGeneration());
            }
            commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            if (deleteFirst || randomBoolean()) {
                closedLocalReadersForGeneration.accept(indexCommit.getGeneration());
            }
        }
    }
}
