/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SearchEngineTests extends ESIntegTestCase {

    public void testFailEngineWithCorruption() throws IOException {
        try (SearchEngine searchEngine = new SearchEngine(config())) {
            assertThat(searchEngine.segments(), empty());
            var store = searchEngine.config().getStore();
            assertThat(store.isMarkedCorrupted(), is(false));

            final Exception exception = randomCorruptionException();
            searchEngine.failEngine("test", exception);

            assertThat(store.isMarkedCorrupted(), is(true));
            expectThrows(AlreadyClosedException.class, searchEngine::segments);
            var listener = searchEngine.config().getEventListener();
            assertThat(listener, instanceOf(FailedEngineEventListener.class));
            assertThat(((FailedEngineEventListener) listener).reason.get(), equalTo("test"));
            assertThat(((FailedEngineEventListener) listener).exception.get(), sameInstance(exception));
        }
    }

    public void testMarkedStoreCorrupted() throws IOException {
        try (SearchEngine searchEngine = new SearchEngine(config())) {
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

    private EngineConfig config() {
        var shardId = new ShardId(new Index("index", "_na_"), randomInt(10));
        var indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        var directory = new SearchDirectory(null, shardId);
        var store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        return new EngineConfig(
            shardId,
            null,
            indexSettings,
            null,
            store,
            null,
            null,
            null,
            null,
            new FailedEngineEventListener(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            () -> { throw new AssertionError(); },
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    private Exception randomCorruptionException() {
        return switch (randomInt(2)) {
            case 0 -> new CorruptIndexException("Test corruption", "test");
            case 1 -> new IndexFormatTooOldException("Test corruption", "test");
            case 2 -> new IndexFormatTooNewException("Test corruption", 0, 0, 0);
            default -> throw new AssertionError("Unexpected value");
        };
    }

    private static class FailedEngineEventListener implements Engine.EventListener {

        final SetOnce<String> reason = new SetOnce<>();
        final SetOnce<Exception> exception = new SetOnce<>();

        @Override
        public void onFailedEngine(String reason, Exception e) {
            this.reason.set(reason);
            this.exception.set(e);
        }
    }
}
