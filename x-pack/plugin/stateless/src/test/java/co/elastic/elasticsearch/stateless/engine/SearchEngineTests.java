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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequestTests;
import co.elastic.elasticsearch.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SearchEngineTests extends AbstractEngineTestCase {

    private static long getCurrentGeneration(SearchEngine engine) {
        return engine.getCurrentPrimaryTermAndGeneration().generation();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testCommitNotifications() throws IOException {
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue, false)
        ) {
            assertThat("Index engine recovery executes 2 commits", indexEngine.getCurrentGeneration(), equalTo(2L));
            assertThat("Search engine recovery executes 1 commit", getCurrentGeneration(searchEngine), equalTo(1L));

            int notifications = notifyCommits(indexEngine, searchEngine);
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
            assertThat(getCurrentGeneration(searchEngine), equalTo(1L));
            assertThat("Index engine is 1 commit ahead after recovery", notifications, equalTo(1));

            searchTaskQueue.runAllRunnableTasks();

            assertThat(getCurrentGeneration(searchEngine), equalTo(indexEngine.getCurrentGeneration()));
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo(0L));

            final int flushes = randomBoolean() ? randomInt(20) : 0;
            if (flushes > 0) {
                for (int i = 0; i < flushes; i++) {
                    indexEngine.index(randomDoc(String.valueOf(i)));
                    indexEngine.flush();
                }
            }
            assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + flushes));

            notifications = notifyCommits(indexEngine, searchEngine);
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
            assertThat(getCurrentGeneration(searchEngine), equalTo(2L));
            assertThat(notifications, equalTo(flushes));

            searchTaskQueue.runAllRunnableTasks();

            assertThat(getCurrentGeneration(searchEngine), equalTo(indexEngine.getCurrentGeneration()));
            assertThat(getCurrentGeneration(searchEngine), equalTo(1L + 1L + flushes));
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo(0L));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testCommitNotificationsAfterCorruption() throws IOException {
        final var indexConfig = indexConfig();

        try (var indexEngine = newIndexEngine(indexConfig)) {
            final int flushesBeforeCorruption = randomBoolean() ? randomInt(20) : 0;
            if (flushesBeforeCorruption > 0) {
                for (int i = 0; i < flushesBeforeCorruption; i++) {
                    indexEngine.index(randomDoc(String.valueOf(i)));
                    indexEngine.flush();
                }
                assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + flushesBeforeCorruption));
            }

            final var searchTaskQueue = new DeterministicTaskQueue();
            try (var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue, false)) {
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();

                final long searchGenerationBeforeCorruption = getCurrentGeneration(searchEngine);
                assertThat(searchGenerationBeforeCorruption, equalTo(indexEngine.getCurrentGeneration()));

                final int flushesAfterCorruption = randomIntBetween(1, 20);
                for (int i = 0; i < flushesAfterCorruption; i++) {
                    indexEngine.index(randomDoc(String.valueOf(i)));
                    indexEngine.flush();
                }
                assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + flushesBeforeCorruption + flushesAfterCorruption));

                int notifications = notifyCommits(indexEngine, searchEngine);
                assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
                assertThat(getCurrentGeneration(searchEngine), equalTo(searchGenerationBeforeCorruption));

                searchEngine.failEngine("test", randomCorruptionException());
                // Pending notifications are not processed once engine is closed.
                searchTaskQueue.runAllRunnableTasks();
                assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
                assertThat(getCurrentGeneration(searchEngine), equalTo(searchGenerationBeforeCorruption));
            }
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testFailEngineWithCorruption() throws IOException {
        final SearchEngine searchEngine = newSearchEngine();
        try (searchEngine) {
            assertThat(searchEngine.segments(), empty());
            var store = searchEngine.config().getStore();
            assertThat(store.isMarkedCorrupted(), is(false));

            final Exception exception = randomCorruptionException();
            searchEngine.failEngine("test", exception);

            assertThat(store.isMarkedCorrupted(), is(true));
            expectThrows(AlreadyClosedException.class, searchEngine::segments);
            var listener = searchEngine.config().getEventListener();
            assertThat(listener, instanceOf(CapturingEngineEventListener.class));
            assertThat(((CapturingEngineEventListener) listener).reason.get(), equalTo("test"));
            assertThat(((CapturingEngineEventListener) listener).exception.get(), sameInstance(exception));
        }
        verify(sharedBlobCacheService).forceEvict(any());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testMarkedStoreCorrupted() throws IOException {
        final SearchEngine searchEngine = newSearchEngine();
        try (searchEngine) {
            var store = searchEngine.config().getStore();
            assertThat(store.isMarkedCorrupted(), is(false));
            var directory = SearchDirectory.unwrapDirectory(store.directory());
            assertThat(directory.isMarkedAsCorrupted(), is(false));
            var files = directory.listAll();
            assertThat(files, arrayWithSize(1));

            store.markStoreCorrupted(new IOException(randomCorruptionException()));

            assertThat(store.isMarkedCorrupted(), is(true));
            assertThat(directory.isMarkedAsCorrupted(), is(true));
            assertThat(directory.listAll(), arrayWithSize(files.length + 1));
            assertThat(Stream.of(directory.listAll()).filter(s -> s.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)).count(), equalTo(1L));
        }
        verify(sharedBlobCacheService).forceEvict(any());
    }

    private Exception randomCorruptionException() {
        return switch (randomInt(2)) {
            case 0 -> new CorruptIndexException("Test corruption", "test");
            case 1 -> new IndexFormatTooOldException("Test corruption", "test");
            case 2 -> new IndexFormatTooNewException("Test corruption", 0, 0, 0);
            default -> throw new AssertionError("Unexpected value");
        };
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testAcquiredPrimaryTermAndGenerations() throws IOException {
        final AtomicLong primaryTerm = new AtomicLong(randomLongBetween(1L, 1_000L));
        final var indexConfig = indexConfig(Settings.EMPTY, Settings.EMPTY, primaryTerm::get);
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue, randomBoolean())
        ) {
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            var gen = new PrimaryTermAndGeneration(primaryTerm.get(), 2L);

            // initial commit acquired
            assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen));
            assertThat(
                searchEngine.getCurrentPrimaryTermAndGeneration(),
                equalTo(new PrimaryTermAndGeneration(primaryTerm.get(), indexEngine.getCurrentGeneration()))
            );

            {
                // new searcher is recorded
                var searcher = searchEngine.acquireSearcher("test");
                assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen));
                searcher.close();
            }

            {
                // record is removed when all searchers are closed
                var searchers = IntStream.range(0, randomIntBetween(2, 50))
                    .mapToObj(i -> searchEngine.acquireSearcher("searcher#" + i))
                    .collect(Collectors.toCollection(ArrayList::new));
                searchers.forEach(searcher -> assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen)));

                Collections.shuffle(searchers, random());

                indexEngine.index(randomDoc("a"));
                indexEngine.flush();

                var gen2 = new PrimaryTermAndGeneration(primaryTerm.get(), gen.generation() + 1);

                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();

                var it = searchers.iterator();
                while (it.hasNext()) {
                    IOUtils.close(it.next());
                    assertThat(
                        searchEngine.getAcquiredPrimaryTermAndGenerations(),
                        it.hasNext() ? containsInAnyOrder(gen, gen2) : contains(gen2)
                    );
                }
            }

            {
                // multiple generations are tracked
                final Map<PrimaryTermAndGeneration, Engine.Searcher> searchers = new HashMap<>();

                final int newGenerations = randomIntBetween(1, 50);
                for (int i = 0; i < newGenerations; i++) {

                    searchers.forEach((key, ignored) -> assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), hasItem(key)));

                    indexEngine.index(randomDoc(String.valueOf(i)));
                    indexEngine.flush();
                    notifyCommits(indexEngine, searchEngine);
                    searchTaskQueue.runAllRunnableTasks();

                    if (randomBoolean() && searchers.isEmpty() == false) {
                        var key = randomFrom(searchers.keySet());
                        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), hasItem(key));

                        IOUtils.close(searchers.remove(key));
                        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), not(hasItem(key)));
                    }

                    if (randomBoolean()) {
                        var key = new PrimaryTermAndGeneration(primaryTerm.get(), indexEngine.getCurrentGeneration());
                        searchers.put(key, searchEngine.acquireSearcher("searcher#" + i));
                    }
                }

                assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + newGenerations + 1L));
                IOUtils.close(searchers.values());
                assertThat(
                    searchEngine.getAcquiredPrimaryTermAndGenerations(),
                    contains(new PrimaryTermAndGeneration(primaryTerm.get(), indexEngine.getCurrentGeneration()))
                );
            }
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testDoesNotRegisterListenersWithOldPrimaryTerm() throws Exception {
        AtomicLong primaryTerm = new AtomicLong(randomLongBetween(1L, 100L));
        long initialPrimaryTerm = primaryTerm.get();
        var indexConfig = indexConfig(Settings.EMPTY, Settings.EMPTY, primaryTerm::get);
        var searchTaskQueue = new DeterministicTaskQueue();
        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue, false)
        ) {
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat(indexEngine.getCurrentGeneration(), equalTo(2L));
            assertThat(searchEngine.getCurrentPrimaryTermAndGeneration(), equalTo(new PrimaryTermAndGeneration(initialPrimaryTerm, 2L)));

            // Listener for existing generation completes immediately
            AtomicBoolean initialTermSecondGenerationListenerCalled = new AtomicBoolean(false);
            assertFalse(
                searchEngine.addOrExecuteSegmentGenerationListener(
                    new PrimaryTermAndGeneration(initialPrimaryTerm, 2L),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long l) {
                            assertThat(l, equalTo(2L));
                            initialTermSecondGenerationListenerCalled.set(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false;
                        }
                    }
                )
            );
            assertTrue(initialTermSecondGenerationListenerCalled.get());

            // Listener for a future primary term and existing generation shouldn't be executed immediately
            AtomicBoolean nextTermSecondGenerationListenerCalled = new AtomicBoolean(false);
            assertTrue(
                searchEngine.addOrExecuteSegmentGenerationListener(
                    new PrimaryTermAndGeneration(initialPrimaryTerm + 1, 2L),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long l) {
                            assertThat(l, equalTo(3L));
                            nextTermSecondGenerationListenerCalled.set(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false;
                        }
                    }
                )
            );
            assertFalse(nextTermSecondGenerationListenerCalled.get());

            // The listener for a future generation is expected to be executed in the future
            AtomicBoolean initialTermThirdGenerationListenerCalled = new AtomicBoolean(false);
            assertTrue(
                searchEngine.addOrExecuteSegmentGenerationListener(
                    new PrimaryTermAndGeneration(initialPrimaryTerm, 3L),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long l) {
                            assertThat(l, equalTo(3L));
                            initialTermThirdGenerationListenerCalled.set(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false;
                        }
                    }
                )
            );
            assertFalse(initialTermThirdGenerationListenerCalled.get());

            // Listener for a future primary term and future generation is expected to be executed in the future
            AtomicBoolean nextTermFourthGenerationListenerCalled = new AtomicBoolean(false);
            assertTrue(
                searchEngine.addOrExecuteSegmentGenerationListener(
                    new PrimaryTermAndGeneration(initialPrimaryTerm + 1, 4L),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long l) {
                            assertThat(l, equalTo(4L));
                            nextTermFourthGenerationListenerCalled.set(true);
                        }

                        @Override
                        public void onFailure(Exception e) {}
                    }
                )
            );
            assertFalse(nextTermFourthGenerationListenerCalled.get());

            // Bump the primary term and generation and sync it with the search engine
            primaryTerm.incrementAndGet();
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                indexEngine.index(randomDoc(String.valueOf(i)));
            }
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            // The search engine has moved to the next primary term
            assertThat(
                searchEngine.getCurrentPrimaryTermAndGeneration(),
                equalTo(new PrimaryTermAndGeneration(initialPrimaryTerm + 1, 3L))
            );
            // The listener for the current term and past generation has been called
            assertTrue(nextTermSecondGenerationListenerCalled.get());
            // The listener for the previous primary term and current generation has been called
            assertTrue(initialTermThirdGenerationListenerCalled.get());
            // The listener for the current term and future generation has NOT been called
            assertFalse(nextTermFourthGenerationListenerCalled.get());

            // The listener for the initial primary term now executes immediately since the
            // search engine has moved on the next term
            AtomicBoolean initialTermFourthGenerationCalled = new AtomicBoolean(false);
            assertFalse(
                searchEngine.addOrExecuteSegmentGenerationListener(
                    new PrimaryTermAndGeneration(initialPrimaryTerm, 4L),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long l) {
                            assertThat(l, equalTo(3L));
                            initialTermFourthGenerationCalled.set(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false;
                        }
                    }
                )
            );
            assertTrue(initialTermFourthGenerationCalled.get());
        }
    }

    /**
     * Test that pruning file metadata in SearchDirectory works. We do this through 3 means:
     * 1. Check that segments_N files are deleted as appropriate.
     * 2. Check that checkIndex does not fail (i.e., check we still have consistent set of files).
     * 3. Check that checkIndex does fail when removing one random (non-segments_N) file
     *    (i.e., check that we removed all the necessary files).
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1297") // amongst others
    public void testPruneFileMetadata() throws IOException {
        final AtomicLong primaryTerm = new AtomicLong(randomLongBetween(1L, 1_000L));
        final var indexConfig = indexConfig(Settings.EMPTY, Settings.EMPTY, primaryTerm::get);
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue, randomBoolean())
        ) {
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            var generation = 2L;

            assertThat(segmentNFiles(searchEngine), equalTo(Set.of(segmentsNFileName(generation))));

            CheckIndex checkIndex = new CheckIndex(searchEngine.getEngineConfig().getStore().directory());
            assertThat(checkIndex.checkIndex().clean, is(true));

            int commits = between(1, 20);
            Map<String, Engine.Searcher> searchers = new HashMap<>();
            for (int i = 0; i < commits; ++i) {
                indexEngine.index(randomDoc(String.valueOf(i)));
                indexEngine.flush();
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();
                generation++;

                assertThat(segmentNFiles(searchEngine), equalTo(Sets.union(Set.of(segmentsNFileName(generation)), searchers.keySet())));
                assertThat(checkIndex.checkIndex().clean, is(true));

                if (randomBoolean()) {
                    searchers.put(segmentsNFileName(generation), searchEngine.acquireSearcher("searcher#" + i));
                }
                if (randomBoolean() && searchers.isEmpty() == false) {
                    var file = randomFrom(searchers.keySet());
                    searchers.remove(file).close();
                }
            }
            assertThat(indexEngine.getCurrentGeneration(), equalTo(generation));

            IOUtils.close(searchers.values());

            // commit once more to trigger file pruning.
            indexEngine.index(randomDoc("final"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            generation++;

            assertThat(segmentNFiles(searchEngine), equalTo(Set.of(segmentsNFileName(generation))));
            assertThat(checkIndex.checkIndex().clean, is(true));

            List<String> files = listFiles(searchEngine);
            String fileToRemove = randomValueOtherThan(segmentsNFileName(generation), () -> randomFrom(files));
            SearchDirectory directory = SearchDirectory.unwrapDirectory(searchEngine.getEngineConfig().getStore().directory());
            directory.retainFiles(
                files.stream()
                    .filter(f -> f.equals(fileToRemove) == false && f.startsWith(IndexFileNames.SEGMENTS))
                    .collect(Collectors.toSet())
            );

            assertThat(checkIndex.checkIndex().clean, is(false));
        }
    }

    public void testLatestUploadedTermAndGenUpdatedOnCommitNotification() throws IOException {
        final var objectStoreUploadTracker = spy(new AtomicMutableObjectStoreUploadTracker());
        final SearchEngine searchEngine = spy(newSearchEngineFromIndexEngine(searchConfig(objectStoreUploadTracker)));
        // Disable commit notification processing to show latestUploadedGen does not depend on it
        doReturn(false).when(searchEngine).addOrExecuteSegmentGenerationListener(any(), anyActionListener());
        try (searchEngine) {
            final var indexShardRoutingTable = NewCommitNotificationRequestTests.randomIndexShardRoutingTable();
            final long primaryTerm = randomLongBetween(10, 42);
            final long ccGen = randomLongBetween(10, 100);
            final long bccGen = randomLongBetween(5, ccGen);

            // 1st notification for commit creation
            final long latestUploadedGen = bccGen - 1;
            final var latestUploadedTermAndGen = new PrimaryTermAndGeneration(primaryTerm, latestUploadedGen);
            final var latestUploadedTermAndGenPlus1 = new PrimaryTermAndGeneration(primaryTerm, latestUploadedGen + 1);

            final var compoundCommit = buildCompoundCommit(indexShardRoutingTable.shardId(), primaryTerm, ccGen);
            searchEngine.onCommitNotification(
                new NewCommitNotification(compoundCommit, bccGen, latestUploadedTermAndGen, 1L, "_node_id"),
                new PlainActionFuture<>()
            );
            verify(objectStoreUploadTracker, times(1)).updateLatestUploadInfo(
                latestUploadedTermAndGen,
                compoundCommit.primaryTermAndGeneration(),
                "_node_id"
            );
            var uploadInfoGen = objectStoreUploadTracker.getLatestUploadInfo(latestUploadedTermAndGen);
            assertThat(uploadInfoGen.isUploaded(), is(true));
            var uploadInfoGenPlus1 = objectStoreUploadTracker.getLatestUploadInfo(latestUploadedTermAndGenPlus1);
            assertThat(uploadInfoGenPlus1.isUploaded(), is(false));

            // 2nd notification for commit upload
            Mockito.clearInvocations(objectStoreUploadTracker);
            searchEngine.onCommitNotification(
                new NewCommitNotification(compoundCommit, bccGen, latestUploadedTermAndGenPlus1, 1L, "_node_id"),
                new PlainActionFuture<>()
            );
            verify(objectStoreUploadTracker, times(1)).updateLatestUploadInfo(
                latestUploadedTermAndGenPlus1,
                compoundCommit.primaryTermAndGeneration(),
                "_node_id"
            );
            assertThat(uploadInfoGen.isUploaded(), is(true));
            assertThat(uploadInfoGenPlus1.isUploaded(), is(true));

            // 3rd notification is a delayed one that has smaller uploaded bcc gen, ensure we do not go backwards in terms of tracking
            Mockito.clearInvocations(objectStoreUploadTracker);
            final var latestUploadedTermAndGenMinusN = new PrimaryTermAndGeneration(
                primaryTerm,
                latestUploadedGen - randomLongBetween(0, 2)
            );
            var compoundCommitGenMinusN = buildCompoundCommit(indexShardRoutingTable.shardId(), primaryTerm, ccGen - 1);
            searchEngine.onCommitNotification(
                new NewCommitNotification(
                    compoundCommitGenMinusN,
                    latestUploadedTermAndGenMinusN.generation(),
                    latestUploadedTermAndGenMinusN,
                    1L,
                    "_node_id"
                ),
                new PlainActionFuture<>()
            );
            verify(objectStoreUploadTracker, times(1)).updateLatestUploadInfo(
                latestUploadedTermAndGenMinusN,
                compoundCommitGenMinusN.primaryTermAndGeneration(),
                "_node_id"
            );
            var uploadInfoGenMinusN = objectStoreUploadTracker.getLatestUploadInfo(latestUploadedTermAndGenMinusN);
            assertThat(uploadInfoGenMinusN.isUploaded(), is(true));
            assertThat(uploadInfoGen.isUploaded(), is(true));
            assertThat(uploadInfoGenPlus1.isUploaded(), is(true));
        }
    }

    private StatelessCompoundCommit buildCompoundCommit(ShardId shardId, long primaryTerm, long ccGen) {
        return new StatelessCompoundCommit(shardId, ccGen, primaryTerm, randomUUID(), Map.of(), randomLongBetween(10, 100), Set.of());
    }

    private static List<String> listFiles(Engine engine) {
        try {
            return List.of(engine.getEngineConfig().getStore().directory().listAll());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> segmentNFiles(SearchEngine searchEngine) {
        return listFiles(searchEngine).stream().filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).collect(Collectors.toSet());
    }

    private static String segmentsNFileName(long generation) {
        return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation);
    }
}
