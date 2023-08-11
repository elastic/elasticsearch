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

import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SearchEngineTests extends AbstractEngineTestCase {

    public void testCommitNotifications() throws IOException {
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            assertThat("Index engine recovery executes 2 commits", indexEngine.getCurrentGeneration(), equalTo(2L));
            assertThat("Search engine recovery executes 1 commit", searchEngine.getCurrentGeneration(), equalTo(1L));

            int notifications = notifyCommits(indexEngine, searchEngine);
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
            assertThat(searchEngine.getCurrentGeneration(), equalTo(1L));
            assertThat("Index engine is 1 commit ahead after recovery", notifications, equalTo(1));

            searchTaskQueue.runAllRunnableTasks();

            assertThat(searchEngine.getCurrentGeneration(), equalTo(indexEngine.getCurrentGeneration()));
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
            assertThat(searchEngine.getCurrentGeneration(), equalTo(2L));
            assertThat(notifications, equalTo(flushes));

            searchTaskQueue.runAllRunnableTasks();

            assertThat(searchEngine.getCurrentGeneration(), equalTo(indexEngine.getCurrentGeneration()));
            assertThat(searchEngine.getCurrentGeneration(), equalTo(1L + 1L + flushes));
            assertThat(searchEngine.getPendingCommitNotifications(), equalTo(0L));
        }
    }

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
            try (var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)) {
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();

                final long searchGenerationBeforeCorruption = searchEngine.getCurrentGeneration();
                assertThat(searchGenerationBeforeCorruption, equalTo(indexEngine.getCurrentGeneration()));

                final int flushesAfterCorruption = randomIntBetween(1, 20);
                for (int i = 0; i < flushesAfterCorruption; i++) {
                    indexEngine.index(randomDoc(String.valueOf(i)));
                    indexEngine.flush();
                }
                assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + flushesBeforeCorruption + flushesAfterCorruption));

                int notifications = notifyCommits(indexEngine, searchEngine);
                assertThat(searchEngine.getPendingCommitNotifications(), equalTo((long) notifications));
                assertThat(searchEngine.getCurrentGeneration(), equalTo(searchGenerationBeforeCorruption));

                searchEngine.failEngine("test", randomCorruptionException());
                searchTaskQueue.runAllRunnableTasks();

                assertThat(searchEngine.getPendingCommitNotifications(), equalTo(0L));
                assertThat(searchEngine.getCurrentGeneration(), equalTo(searchGenerationBeforeCorruption));
            }
        }
    }

    public void testFailEngineWithCorruption() throws IOException {
        try (SearchEngine searchEngine = newSearchEngine()) {
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
    }

    public void testMarkedStoreCorrupted() throws IOException {
        try (SearchEngine searchEngine = newSearchEngine()) {
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
    }

    private Exception randomCorruptionException() {
        return switch (randomInt(2)) {
            case 0 -> new CorruptIndexException("Test corruption", "test");
            case 1 -> new IndexFormatTooOldException("Test corruption", "test");
            case 2 -> new IndexFormatTooNewException("Test corruption", 0, 0, 0);
            default -> throw new AssertionError("Unexpected value");
        };
    }

    public void testAcquiredPrimaryTermAndGenerations() throws IOException {
        final AtomicLong primaryTerm = new AtomicLong(randomLongBetween(1L, 1_000L));
        final var indexConfig = indexConfig(Settings.EMPTY, Settings.EMPTY, primaryTerm::get);
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            var gen = new PrimaryTermAndGeneration(primaryTerm.get(), 2L);

            // no initial active searchers
            assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), empty());
            assertThat(searchEngine.getCurrentGeneration(), equalTo(indexEngine.getCurrentGeneration()));

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

                var it = searchers.iterator();
                while (it.hasNext()) {
                    IOUtils.close(it.next());
                    assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), it.hasNext() ? contains(gen) : empty());
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

                    if (randomBoolean()) {
                        notifyCommits(indexEngine, searchEngine);
                        searchTaskQueue.runAllRunnableTasks();

                        var key = new PrimaryTermAndGeneration(primaryTerm.get(), indexEngine.getCurrentGeneration());
                        searchers.put(key, searchEngine.acquireSearcher("searcher#" + i));
                    }

                    if (randomBoolean() && searchers.isEmpty() == false) {
                        var key = randomFrom(searchers.keySet());
                        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), hasItem(key));

                        IOUtils.close(searchers.remove(key));
                        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), not(hasItem(key)));
                    }
                }

                assertThat(indexEngine.getCurrentGeneration(), equalTo(2L + newGenerations));
                IOUtils.close(searchers.values());
                assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), empty());
            }
        }
    }
}
