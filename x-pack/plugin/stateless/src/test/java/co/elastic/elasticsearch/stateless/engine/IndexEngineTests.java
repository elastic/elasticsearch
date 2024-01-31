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

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.translog.Translog;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexEngineTests extends AbstractEngineTestCase {

    public void testAsyncEnsureSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            Translog.Location location = new Translog.Location(0, 0, 0);
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            doAnswer((Answer<Void>) invocation -> {
                future.onResponse(null);
                return null;
            }).when(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            engine.asyncEnsureTranslogSynced(location, e -> {
                if (e == null) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            });
            verify(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            future.actionGet();
        }
    }

    public void testSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            doAnswer((Answer<Void>) invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(replicator).syncAll(eq(engine.config().getShardId()), any());
            engine.syncTranslog();
            verify(replicator).syncAll(eq(engine.config().getShardId()), any());
        }
    }

    public void testSyncIsNeededIfTranslogReplicatorHasUnsyncedData() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            assertFalse(engine.isTranslogSyncNeeded());
            when(replicator.isSyncNeeded(engine.config().getShardId())).thenReturn(true);
            assertTrue(engine.isTranslogSyncNeeded());
        }
    }

    public void testRefreshNeededForFastRefreshesBasedOnSearcher() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            assertFalse(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            engine.flush();
            assertTrue(engine.refreshNeeded());
        }
    }

    public void testRefreshNeededForNonFastRefreshesBasedOnSearcherAndCommits() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            // Need refresh because only maybeRefresh / externalRefreshes commit
            assertTrue(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            // Still need refresh
            assertTrue(engine.refreshNeeded());
            if (randomBoolean()) {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.externalRefresh("test", future);
                future.actionGet();
            } else {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.maybeRefresh("test", future);
                future.actionGet();
            }
            assertFalse(engine.refreshNeeded());
        }
    }

    public void testStartingTranslogFileIsPlaceInUserData() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        TranslogReplicator replicator = mock(TranslogReplicator.class);
        long maxUploadedFile = randomLongBetween(10, 20);
        when(replicator.getMaxUploadedFile()).thenReturn(maxUploadedFile, maxUploadedFile + 1);

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings
                ),
                replicator
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            engine.flush();
            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(
                    maxUploadedFile + 1,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
            }
        }
    }

    /**
     * Test that the engine calls the closed reader notification for every reader that it no longer uses.
     */
    public void testClosedReaders() throws IOException {
        TranslogReplicator translogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService commitService = mockCommitService();
        List<Long> closedReaderGenerations = new ArrayList<>();
        when(commitService.closedLocalReadersForGeneration(any())).thenReturn(closedReaderGenerations::add);
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                ),
                translogReplicator,
                mock(ObjectStoreService.class),
                commitService
            )
        ) {
            final var initialGen = engine.getCurrentGeneration();
            engine.index(randomDoc(String.valueOf(0)));
            if (randomBoolean()) {
                engine.refresh("test");
            }
            engine.flush(true, true);
            assertThat(closedReaderGenerations, empty());
            // external reader manager refresh.
            engine.refresh("test");
            List<Long> expectedClosed = new ArrayList<>();
            expectedClosed.add(initialGen);
            assertThat(closedReaderGenerations, equalTo(expectedClosed));

            final var gen = engine.getCurrentGeneration();
            int commits = between(1, 10);
            NavigableMap<Long, Engine.Searcher> searchers = new TreeMap<>();
            for (int i = 0; i < commits; ++i) {
                if (randomBoolean()) {
                    searchers.put(i + gen, engine.acquireSearcher("test"));
                }
                engine.index(randomDoc(String.valueOf(i + 1)));
                engine.flush(true, true);
                engine.refresh("test");
            }

            Runnable refreshExpectedClosed = () -> {
                long releasedUntil = searchers.isEmpty() ? commits + gen : searchers.firstKey();
                for (long generation = expectedClosed.get(expectedClosed.size() - 1) + 1; generation < releasedUntil; ++generation) {
                    expectedClosed.add(generation);
                }
            };
            refreshExpectedClosed.run();
            assertThat(closedReaderGenerations, equalTo(expectedClosed));

            while (searchers.isEmpty() == false) {
                long generation = randomFrom(searchers.keySet());
                Engine.Searcher searcher = searchers.remove(generation);
                searcher.close();
                refreshExpectedClosed.run();
                assertThat(closedReaderGenerations, equalTo(expectedClosed));
            }
        }
    }
}
