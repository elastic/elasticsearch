/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with one
 * of these licenses. You may obtain a copy of the licenses at
 * https://www.elastic.co/licensing/elastic-license
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class BatchMidBatchFailureIT extends ESIntegTestCase {

    private static final String SIMULATED_LOOKUP_FAILURE = "simulated version lookup failure";
    private static final String SIMULATED_WRITE_FAILURE = "simulated write failure";

    /**
     * Breaks a primary batch at one of the two points the engine distinguishes.
     * <ul>
     * <li>{@link #failLookup}: the n-th Lucene version lookup from now on throws. Primary planning resolves the ids of a
     * sub-batch that are missing from the version map with one such lookup, so this breaks the planning of a chosen
     * sub-batch while the engine stays healthy.</li>
     * <li>{@link #failWrite}: the n-th Lucene write from now on throws, while the writer reports a different tragic event.
     * A sub-batch of new documents is one columnar write and a sub-batch with an update writes document by document, so
     * this breaks a chosen sub-batch after its sequence numbers were reserved. The engine can neither treat the failure as
     * a document failure nor match it to the tragic event, so it has to fail itself.</li>
     * </ul>
     */
    public static class FailingEnginePlugin extends Plugin implements EnginePlugin {

        private static final AtomicInteger lookupsUntilFailure = new AtomicInteger(-1);
        private static final AtomicInteger writesUntilFailure = new AtomicInteger(-1);

        static void failLookup(int n) {
            lookupsUntilFailure.set(n);
        }

        static void failWrite(int n) {
            writesUntilFailure.set(n);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                protected <T> T performActionWithDirectoryReader(
                    Engine.SearcherScope scope,
                    CheckedFunction<DirectoryReader, T, IOException> action
                ) throws EngineException {
                    if (lookupsUntilFailure.decrementAndGet() == 0) {
                        throw new EngineException(shardId, SIMULATED_LOOKUP_FAILURE);
                    }
                    return super.performActionWithDirectoryReader(scope, action);
                }

                @Override
                protected IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    // a plain IndexWriter: the engine's own subclass only forwards tragic events to the merge scheduler and
                    // asserts against hard deletes, neither of which these tests exercise
                    return new IndexWriter(directory, iwc) {
                        private volatile Throwable tragic;

                        private void maybeFailWrite() throws IOException {
                            if (writesUntilFailure.decrementAndGet() == 0) {
                                tragic = new IOException("tragic event recorded by another thread");
                                throw new IOException(SIMULATED_WRITE_FAILURE);
                            }
                        }

                        @Override
                        public long addBatch(ColumnBatch columnBatch) throws IOException {
                            maybeFailWrite();
                            return super.addBatch(columnBatch);
                        }

                        @Override
                        public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
                            maybeFailWrite();
                            return super.addDocuments(docs);
                        }

                        @Override
                        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes)
                            throws IOException {
                            maybeFailWrite();
                            return super.softUpdateDocument(term, doc, softDeletes);
                        }

                        @Override
                        public Throwable getTragicException() {
                            return tragic != null ? tragic : super.getTragicException();
                        }
                    };
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // InternalSettingsPlugin registers index.global_checkpoint_sync.interval
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), FailingEnginePlugin.class),
            InternalSettingsPlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false; // only one EnginePlugin may provide an engine factory
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), BatchIndexingEnabled.FEATURE_FLAG.isEnabled())
            .build();
    }

    public void testPlanningFailureMidBatchFailsRemainingItemsOnly() throws Exception {
        final String index = createColumnarIndex();

        // [a, b, a, c]: the duplicate id splits the batch into the sub-batches [a, b] and [a, c]. Each one resolves its ids that
        // are missing from the version map with one Lucene lookup (a and b, then only c), so failing the second lookup breaks
        // the planning of the second sub-batch once the first one is indexed.
        FailingEnginePlugin.failLookup(2);
        final BulkResponse response = client().bulk(
            new BulkRequest().add(doc(index, "a"), doc(index, "b"), doc(index, "a"), doc(index, "c"))
        ).actionGet();

        // [a, b] succeeded with sequence numbers 0 and 1; [a, c] were failed with the lookup error and consumed no sequence number
        final BulkItemResponse[] items = response.getItems();
        assertThat(items.length, equalTo(4));
        for (int i = 0; i < 2; i++) {
            assertFalse(items[i].getFailureMessage(), items[i].isFailed());
            assertThat(items[i].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(items[i].getResponse().getSeqNo(), equalTo((long) i));
        }
        for (int i = 2; i < 4; i++) {
            assertTrue(items[i].isFailed());
            final Throwable cause = ExceptionsHelper.unwrapCause(items[i].getFailure().getCause());
            assertThat(cause, instanceOf(EngineException.class));
            assertThat(cause.getMessage(), containsString(SIMULATED_LOOKUP_FAILURE));
            assertThat(items[i].getFailure().getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        }

        // the engine is still open and the first sub-batch was replicated: both copies have a and b, and no checkpoint stalls
        ensureGreen(index);
        assertBothCopies(index, 1L, 2L);

        // the failed items can simply be retried; the update of a and the new c take the next sequence numbers
        final BulkResponse retry = client().bulk(new BulkRequest().add(doc(index, "a"), doc(index, "c"))).actionGet();
        assertFalse(retry.buildFailureMessage(), retry.hasFailures());
        assertThat(retry.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat(retry.getItems()[0].getResponse().getSeqNo(), equalTo(2L));
        assertThat(retry.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(retry.getItems()[1].getResponse().getSeqNo(), equalTo(3L));
        assertBothCopies(index, 3L, 3L);
    }

    public void testFailureAfterSeqNoReservationFailsThePrimary() throws Exception {
        final String index = createColumnarIndex();
        final BulkResponse first = client().bulk(new BulkRequest().add(doc(index, "a"), doc(index, "b"))).actionGet();
        assertFalse(first.buildFailureMessage(), first.hasFailures());
        assertBothCopies(index, 1L, 2L);
        final String primaryNodeId = shardTable(index).primaryShard().currentNodeId();

        // only allocate primaries
        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries")
        );
        try {
            // This will fail LuceneIndex write
            FailingEnginePlugin.failWrite(1);
            final BulkResponse response = client().bulk(new BulkRequest().add(doc(index, "c"), doc(index, "d"))).actionGet();

            // the engine is gone, so the shard request fails as a whole and every item carries the write failure
            // primary will fail, replica will be promoted, but new replica is not allocated
            assertTrue(response.hasFailures());
            for (BulkItemResponse item : response.getItems()) {
                assertTrue(item.isFailed());
                final Throwable cause = ExceptionsHelper.unwrapCause(item.getFailure().getCause());
                assertThat(cause.getMessage(), containsString(SIMULATED_WRITE_FAILURE));
            }

            // the primary was failed because its sub-batch did not complete, the replica was promoted and the index is yellow
            assertBusy(() -> {
                final IndexShardRoutingTable shardTable = shardTable(index);
                final ShardRouting primary = shardTable.primaryShard();
                assertTrue(primary.toString(), primary.started());
                assertThat(primary.currentNodeId(), not(equalTo(primaryNodeId)));
                final ShardRouting replica = shardTable.replicaShards().get(0);
                assertTrue(replica.toString(), replica.unassigned());
                assertThat(replica.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
                assertThat(replica.unassignedInfo().message(), containsString("did not complete after its sequence numbers were reserved"));
            });
            assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, index).get().getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        } finally {
            updateClusterSettings(Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
        }

        // the failed copy recovers from the promoted primary; the failed operations never made it into any translog
        ensureGreen(index);
        assertBothCopies(index, 1L, 2L);

        // and the new primary accepts them
        final BulkResponse retry = client().bulk(new BulkRequest().add(doc(index, "c"), doc(index, "d"))).actionGet();
        assertFalse(retry.buildFailureMessage(), retry.hasFailures());
        for (int i = 0; i < 2; i++) {
            assertThat(retry.getItems()[i].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(retry.getItems()[i].getResponse().getSeqNo(), equalTo(2L + i));
        }
        assertBothCopies(index, 3L, 4L);
    }

    public void testCompletedSubBatchesOfFailedRequestAreDiscarded() throws Exception {
        final String index = createColumnarIndex();
        final BulkResponse first = client().bulk(new BulkRequest().add(doc(index, "a"), doc(index, "b"))).actionGet();
        assertFalse(first.buildFailureMessage(), first.hasFailures());
        assertBothCopies(index, 1L, 2L);

        // [c, d, c, e]: the duplicate id splits the request into the sub-batches [c, d] and [c, e].

        // Fail the request after 2 items. This failure is post planning (which reserves the seq nos)
        // This failure comes from IndexWriter.maybeFailWrite

        // The first sub batch will succeed in indexing and the second sub batch will fail tragically, causing the
        // primary to fail.
        // Replication happens at the end of the entire batch request. So, the sub batch that succeeded will also not
        // recover.

        // failPrimaryMidBatch will assert that the entire batch failed and a replica successfully took over
        failPrimaryMidBatch(index, 2, doc(index, "c"), doc(index, "d"), doc(index, "c"), doc(index, "e"));

        // the completed sub-batch was never replicated and never acknowledged, so it must not survive the fail-over on either
        // copy: the promoted primary never had it and the failed copy drops it when it recovers from the promoted primary
        assertBothCopies(index, 1L, 2L);
        assertCopiesHoldExactly(index, Set.of("a", "b"));

        // which is why the retry creates all three, with the next sequence numbers
        final BulkResponse retry = client().bulk(new BulkRequest().add(doc(index, "c"), doc(index, "d"), doc(index, "e"))).actionGet();
        assertFalse(retry.buildFailureMessage(), retry.hasFailures());
        for (int i = 0; i < 3; i++) {
            assertThat(retry.getItems()[i].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(retry.getItems()[i].getResponse().getSeqNo(), equalTo(2L + i));
        }
        assertBothCopies(index, 4L, 5L);
        assertCopiesHoldExactly(index, Set.of("a", "b", "c", "d", "e"));
    }

    public void testAcknowledgedDocsSurviveFailOverAndFailBack() throws Exception {
        final String index = createColumnarIndex();
        final String originalPrimaryNode = shardTable(index).primaryShard().currentNodeId();
        final Set<String> acknowledged = new HashSet<>();

        // Create 200 documents
        indexAcknowledged(acknowledged, bulkOf(index, 0, 200));
        assertCopiesHoldExactly(index, acknowledged);

        // fail over: the primary dies mid-batch, the replica is promoted and the failed copy recovers from it
        failPrimaryMidBatch(index, 1, doc(index, "fo-1"), doc(index, "fo-2"));
        assertThat(shardTable(index).primaryShard().currentNodeId(), not(equalTo(originalPrimaryNode)));
        assertCopiesHoldExactly(index, acknowledged);

        // the promoted primary accepts new docs and the retry of the failed ids, which were never indexed anywhere
        final BulkResponse afterFailOver = indexAcknowledged(
            acknowledged,
            bulkOf(index, 200, 400).add(doc(index, "fo-1")).add(doc(index, "fo-2"))
        );
        assertCreated(afterFailOver, "fo-1", "fo-2");
        assertCopiesHoldExactly(index, acknowledged);

        // fail back: the promoted primary dies the same way and the original node takes over again
        failPrimaryMidBatch(index, 1, doc(index, "fb-1"), doc(index, "fb-2"));
        assertThat(shardTable(index).primaryShard().currentNodeId(), equalTo(originalPrimaryNode));
        assertCopiesHoldExactly(index, acknowledged);

        final BulkResponse afterFailBack = indexAcknowledged(
            acknowledged,
            bulkOf(index, 400, 600).add(doc(index, "fb-1")).add(doc(index, "fb-2"))
        );
        assertCreated(afterFailBack, "fb-1", "fb-2");
        assertCopiesHoldExactly(index, acknowledged);
    }

    private String createColumnarIndex() {
        assumeTrue("batch indexing is behind a feature flag", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        final String index = "columnar";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    indexSettings(1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                        .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                )
                .setMapping("host", "type=keyword", "service", "type=keyword")
        );
        ensureGreen(index);
        return index;
    }

    private static IndexShardRoutingTable shardTable(String index) {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().routingTable().index(index).shard(0);
    }

    /** Asserts the max seq#, local and global checkpoint and the live doc count of the primary and the replica. */
    private static void assertBothCopies(String index, long checkpoint, long docCount) throws Exception {
        indicesAdmin().prepareRefresh(index).get();
        assertBusy(() -> {
            final ShardStats[] shards = indicesAdmin().prepareStats(index).clear().setDocs(true).get().getShards();
            assertThat(shards.length, equalTo(2));
            for (ShardStats shard : shards) {
                final String copy = shard.getShardRouting().toString();
                assertThat(copy, shard.getSeqNoStats().getMaxSeqNo(), equalTo(checkpoint));
                assertThat(copy, shard.getSeqNoStats().getLocalCheckpoint(), equalTo(checkpoint));
                assertThat(copy, shard.getSeqNoStats().getGlobalCheckpoint(), equalTo(checkpoint));
                assertThat(copy, shard.getStats().getDocs().getCount(), equalTo(docCount));
            }
        });
    }

    /** doc-from .. doc-(to-1), ids never seen before: an append-only bulk that takes the columnar write path. */
    private static BulkRequest bulkOf(String index, int from, int to) {
        final BulkRequest bulk = new BulkRequest();
        for (int i = from; i < to; i++) {
            bulk.add(doc(index, "doc-" + i));
        }
        return bulk;
    }

    /** Every item must be acknowledged; their ids join the set the copies are compared against. */
    private static BulkResponse indexAcknowledged(Set<String> acknowledged, BulkRequest bulk) {
        final BulkResponse response = client().bulk(bulk).actionGet();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        for (BulkItemResponse item : response.getItems()) {
            acknowledged.add(item.getId());
        }
        return response;
    }

    private static void assertCreated(BulkResponse response, String... ids) {
        for (String id : ids) {
            final BulkItemResponse item = Arrays.stream(response.getItems()).filter(i -> i.getId().equals(id)).findFirst().orElseThrow();
            assertThat(id, item.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }
    }

    /**
     * Sends the operations as one bulk with {@link FailingEnginePlugin#failWrite}.
     * the primary fails after the sequence numbers of that write's sub-batch were reserved.
     * Waits until the other copy is promoted, then lets the failed copy recover from it.
     * Every item comes back failed: nothing of the bulk is acknowledged.
     */
    private void failPrimaryMidBatch(String index, int failingWrite, IndexRequest... docs) throws Exception {
        final String primaryNode = shardTable(index).primaryShard().currentNodeId();
        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries")
        );
        try {
            FailingEnginePlugin.failWrite(failingWrite);
            final BulkResponse response = client().bulk(new BulkRequest().add(docs)).actionGet();
            assertTrue(response.hasFailures());
            for (BulkItemResponse item : response.getItems()) {
                assertTrue(item.isFailed());
                final Throwable cause = ExceptionsHelper.unwrapCause(item.getFailure().getCause());
                assertThat(cause.getMessage(), containsString(SIMULATED_WRITE_FAILURE));
            }
            assertBusy(() -> {
                final IndexShardRoutingTable shardTable = shardTable(index);
                assertTrue(shardTable.primaryShard().toString(), shardTable.primaryShard().started());
                assertThat(shardTable.primaryShard().currentNodeId(), not(equalTo(primaryNode)));
                assertTrue(shardTable.replicaShards().get(0).toString(), shardTable.replicaShards().get(0).unassigned());
            });
        } finally {
            updateClusterSettings(Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
        }
        ensureGreen(index);
    }

    /**
     * Both copies are in sync (same seq# stats; same live docs with the same seq#, term, version and source) and hold
     * exactly the acknowledged ids, nothing more and nothing less.
     */
    private void assertCopiesHoldExactly(String index, Set<String> acknowledged) throws Exception {
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
        final IndexShardRoutingTable shardTable = shardTable(index);
        for (ShardRouting copy : List.of(shardTable.primaryShard(), shardTable.replicaShards().get(0))) {
            assertThat(copy.toString(), getShardDocIDs(shardOnNode(index, copy)), equalTo(acknowledged));
        }
    }

    private IndexShard shardOnNode(String index, ShardRouting copy) {
        final String nodeName = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .nodes()
            .get(copy.currentNodeId())
            .getName();
        return internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(resolveIndex(index)).getShard(copy.id());
    }

    private static IndexRequest doc(String index, String id) {
        return new IndexRequest(index).id(id)
            .opType(DocWriteRequest.OpType.INDEX)
            .source(XContentType.JSON, "host", "h-" + id, "service", "s-" + id);
    }
}
