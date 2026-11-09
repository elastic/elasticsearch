/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngineDynamicSettings;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isSafeAccessRequired;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * An update's version can go unrecorded in the live version map: the update enforces safe access before writing to Lucene and
 * records its version after, and if a refresh, an append with an auto-generated id, and another refresh land in between, safe access
 * lapses by design and the version is skipped. The stateless archive still holds the doc's previous version until the search shards
 * acknowledge a newer commit, so the next update of the doc resolves its version against that stale entry.
 * <p>
 * What the next update then does depends on whether it can read the stale version's operation from the local translog. That needs
 * translog locations to be tracked, which starts with the first realtime get or update on the shard. Without a flush in
 * between, the update reads the stale version from the translog, is built on it, passes the seq_no check against it and silently
 * drops the previous update. After a flush, the translog no longer holds it, so the update reads the latest doc from Lucene and is
 * rejected by the seq_no check against the stale entry.
 */
public class StatelessUpdateAfterSkippedVersionIT extends AbstractStatelessPluginIntegTestCase {

    /** Runs once, right after the next soft update (an update of an existing doc) has been written to Lucene. */
    private static final AtomicReference<Runnable> ON_SOFT_UPDATE = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    @After
    public void clearHook() {
        ON_SOFT_UPDATE.set(null);
    }

    public void testUpdateKeepsPreviousUpdateWhoseVersionWasSkipped() throws Exception {
        final var test = setUpUpdateWithSkippedVersion();

        // No flush since the skipped update: the translog still holds the previous version's operation.

        // Without the fix: get for update: the current map is unsafe, so the write path refreshes first. That's a local refresh, not a
        // flush.
        // The lookup then finds the archived seq-no 1 for fa, and fa's operation is still in the translog. So it reads {f0, fa} with
        // seq-no 1. It builds {f0, fa, f2} with if_seq_no = 1.
        // Index: the lookup again finds the archived seq-no 1, so the version check passes, and the doc replaces seq-no 2 in Lucene.

        // Without the fix, the end result is silent dropping of f1.
        final UpdateResponse response = client().prepareUpdate(test.indexName, "doc").setDoc("f2", 2).get();
        assertThat(response.getResult(), equalTo(UpdateResponse.Result.UPDATED));

        test.releaseCommitNotifications();
        // Without the fix this fails, and no version conflict is thrown.
        assertSourceHasFields(test.indexName, "f0", "fa", "f1", "f2");
    }

    public void testUpdateAfterFlushSucceedsWhenPreviousVersionWasSkipped() throws Exception {
        final var test = setUpUpdateWithSkippedVersion();

        // The flush trims the translog. The search shard does not acknowledge the new commit yet, so the archive keeps the
        // previous version.
        test.engine.flush(true, true);
        try {
            // Without the fix: get for update: it finds the archived seq-no 1 with L1, but Translog.readOperation(L1) returns null because
            // that generation
            // was trimmed. It falls back to Lucene and reads {f0, fa, f1} with seq-no 2. It builds {f0, fa, f1, f2} with if_seq_no = 2.
            // Index: the lookup finds the archived seq-no 1. 2 != 1, so the update fails with a 409: "required seqNo [2] ... current
            // document has seqNo [1]".
            final UpdateResponse response = client().prepareUpdate(test.indexName, "doc").setDoc("f2", 2).get();
            assertThat(response.getResult(), equalTo(UpdateResponse.Result.UPDATED));
        } catch (VersionConflictEngineException e) {
            // Without the fix this is thrown, e.g. version conflict, required seqNo [2], primary term [1]. current document has seqNo [1]
            // and primary term [1]
            throw new AssertionError("update was rejected by a version conflict against a stale version", e);
        }

        test.releaseCommitNotifications();
        assertSourceHasFields(test.indexName, "f0", "fa", "f1", "f2");
    }

    private record SetUp(String indexName, IndexEngine engine, AtomicBoolean holdCommitNotifications, List<Runnable> heldNotifications) {
        void releaseCommitNotifications() {
            holdCommitNotifications.set(false);
            synchronized (heldNotifications) {
                heldNotifications.forEach(Runnable::run);
                heldNotifications.clear();
            }
        }
    }

    /**
     * Indexes the doc with field f0, updates it with field fa and moves its version into the archive, then updates it with field f1
     * while forcing the update's
     * version to be skipped. Holds back commit notifications to the search shard, so that nothing is pruned from the archive.
     */
    private SetUp setUpUpdateWithSkippedVersion() throws Exception {
        final String indexNode = startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode();
        ensureStableCluster(2);
        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        final AtomicBoolean holdCommitNotifications = new AtomicBoolean(true);
        final List<Runnable> heldNotifications = Collections.synchronizedList(new ArrayList<>());
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if ((TransportNewCommitNotificationAction.NAME + "[u]").equals(action) && holdCommitNotifications.get()) {
                heldNotifications.add(() -> {
                    try {
                        connection.sendRequest(requestId, action, request, options);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return;
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine engine = (IndexEngine) indexShard.getEngineOrNull();

        client().prepareIndex(indexName).setId("doc").setSource("f0", 0).get();
        // The first update finds a version without a translog location and turns on tracking translog locations, as happens on any
        // shard serving realtime gets or updates. Its own version is recorded with a location.
        client().prepareUpdate(indexName, "doc").setDoc("fa", 0).get();
        engine.refresh("test"); // a local refresh: moves the doc's version into the archive, does not commit

        ON_SOFT_UPDATE.set(() -> {
            engine.refresh("test");
            indexAppendOnlyOnAnotherThread(indexShard, engine);
            engine.refresh("test");
            assertFalse("safe access should have lapsed", isSafeAccessRequired(engine.getLiveVersionMap()));
        });
        final UpdateResponse response = client().prepareUpdate(indexName, "doc").setDoc("f1", 1).get();
        assertThat(response.getResult(), equalTo(UpdateResponse.Result.UPDATED));
        assertNull("the update did not write to Lucene", ON_SOFT_UPDATE.get());

        return new SetUp(indexName, engine, holdCommitNotifications, heldNotifications);
    }

    /** Indexes a doc with an auto-generated id directly into the engine, so it does not need a write thread. */
    private static void indexAppendOnlyOnAnotherThread(IndexShard indexShard, IndexEngine engine) {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final Thread indexer = new Thread(() -> {
            try {
                final Engine.Index append = IndexShard.prepareIndex(
                    indexShard.mapperService(),
                    new SourceToParse(UUIDs.base64UUID(), new BytesArray("{\"f0\": 1}"), XContentType.JSON),
                    UNASSIGNED_SEQ_NO,
                    indexShard.getOperationPrimaryTerm(),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.currentTimeMillis(),
                    false,
                    UNASSIGNED_SEQ_NO,
                    0,
                    System.nanoTime()
                );
                assertThat(engine.index(append).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            } catch (Exception e) {
                failure.set(e);
            }
        }, "append-only-indexer");
        indexer.start();
        try {
            indexer.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        assertNull(failure.get());
    }

    private void assertSourceHasFields(String indexName, String... fields) {
        indicesAdmin().prepareRefresh(indexName).get();
        assertResponse(
            client().prepareSearch(indexName).setQuery(org.elasticsearch.index.query.QueryBuilders.idsQuery().addIds("doc")),
            r -> {
                assertThat(r.getHits().getHits().length, equalTo(1));
                final Map<String, Object> source = r.getHits().getHits()[0].getSourceAsMap();
                for (String field : fields) {
                    assertTrue("field [" + field + "] missing from " + source, source.containsKey(field));
                }
            }
        );
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics,
            IndexEngineDynamicSettings indexEngineDynamicSettings
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                indexEngineDynamicSettings,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return new IndexWriter(directory, iwc) {
                        @Override
                        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes)
                            throws IOException {
                            final long seqNo = super.softUpdateDocument(term, doc, softDeletes);
                            final Runnable hook = ON_SOFT_UPDATE.getAndSet(null);
                            if (hook != null) {
                                hook.run();
                            }
                            return seqNo;
                        }
                    };
                }
            };
        }
    }
}
