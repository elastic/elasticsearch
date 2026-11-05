/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newIndexTask;
import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newShardTask;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Exercises {@link DownsampleStateStore} against a real system index: the mappings accept the documents, writes are
 * compare-and-set, creates are idempotent, and the node view is fenced by owner, epoch and target index uuid.
 */
public class DownsampleStateStoreTests extends ESSingleNodeTestCase {

    private DownsampleStateStore store;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class, DataStreamsPlugin.class);
    }

    @Before
    public void setUpStore() {
        assumeTrue("the system index is only registered with the feature flag", DownsampleStateIndex.FEATURE_FLAG.isEnabled());
        store = new DownsampleStateStore(client(), getInstanceFromNode(ClusterService.class));
    }

    public void testReadsBeforeTheIndexExists() {
        assertThat(getIndexTask("target"), equalTo(Optional.empty()));
        assertThat(findActiveIndexTasks(), empty());
        assertThat(findMyShardTasks(), empty());
    }

    public void testCreatesASystemIndex() {
        createIndexTask(newIndexTask("target"));
        IndexMetadata indexMetadata = getInstanceFromNode(ClusterService.class).state()
            .metadata()
            .getProject()
            .index(DownsampleStateIndex.INDEX_NAME);
        assertNotNull(indexMetadata);
        assertTrue(indexMetadata.isSystem());
        assertTrue(indexMetadata.isHidden());
    }

    public void testIndexTaskCreateIsIdempotent() {
        Versioned<IndexTaskDoc> created = createIndexTask(newIndexTask("target"));
        Versioned<IndexTaskDoc> again = createIndexTask(newIndexTask("target"));
        assertThat(again, equalTo(created));
        assertThat(getIndexTask("target"), equalTo(Optional.of(created)));
    }

    public void testIndexTaskUpdateIsCompareAndSet() {
        Versioned<IndexTaskDoc> v1 = createIndexTask(newIndexTask("target"));
        Versioned<IndexTaskDoc> v2 = updateIndexTask(v1, v1.doc().targetCreated(randomUUID()));
        assertThat(v2.seqNo(), greaterThan(v1.seqNo()));
        // a stale writer (e.g. a previous master) still holds v1
        PlainActionFuture<Versioned<IndexTaskDoc>> stale = new PlainActionFuture<>();
        store.forMaster().updateIndexTask(v1, v1.doc().failed("stale"), stale);
        assertVersionConflict(stale);
        assertThat(getIndexTask("target").orElseThrow(), equalTo(v2));
    }

    public void testInvalidIndexTaskTransitionIsNotWritten() {
        Versioned<IndexTaskDoc> v1 = createIndexTask(newIndexTask("target"));
        PlainActionFuture<Versioned<IndexTaskDoc>> invalid = new PlainActionFuture<>();
        store.forMaster().updateIndexTask(v1, v1.doc().withPhase(IndexTaskPhase.RUN_SHARDS), invalid);
        assertRejected(invalid, "requires the target index uuid");
        assertThat(getIndexTask("target").orElseThrow(), equalTo(v1));
    }

    public void testTerminalIndexTaskIsReplaced() {
        Versioned<IndexTaskDoc> v1 = createIndexTask(newIndexTask("target"));
        Versioned<IndexTaskDoc> failed = updateIndexTask(v1, v1.doc().failed("boom"));
        IndexTaskDoc fresh = newIndexTask("target");
        Versioned<IndexTaskDoc> replaced = createIndexTask(fresh);
        assertThat(replaced.doc(), equalTo(fresh));
        assertThat(replaced.seqNo(), greaterThan(failed.seqNo()));
    }

    public void testFindActiveIndexTasks() {
        Versioned<IndexTaskDoc> active = createIndexTask(newIndexTask("active"));
        Versioned<IndexTaskDoc> done = createIndexTask(newIndexTask("done"));
        updateIndexTask(done, done.doc().failed("boom"));
        refresh();
        List<Versioned<IndexTaskDoc>> found = findActiveIndexTasks();
        assertThat(found, hasSize(1));
        assertThat(found.get(0), equalTo(active));
    }

    public void testShardTaskCreateIsIdempotentAndKeepsSourceUuid() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        Versioned<ShardTaskDoc> created = createShardTask(doc);
        Versioned<ShardTaskDoc> again = createShardTask(doc);
        assertThat(again, equalTo(created));
        refresh();
        List<Versioned<ShardTaskDoc>> found = call(l -> store.forMaster().findShardTasks("target", doc.targetIndexUuid(), l));
        assertThat(found, hasSize(1));
        assertThat(found.get(0).doc().sourceShardId().getIndex().getUUID(), equalTo(doc.sourceShardId().getIndex().getUUID()));
    }

    public void testNodeRunsItsTaskAndIsFencedOnReassignment() {
        String targetUuid = createTargetIndex("target");
        String localNode = store.forLocalNode().nodeId();
        Versioned<ShardTaskDoc> created = createShardTask(newShardTask("target", targetUuid));
        Versioned<ShardTaskDoc> assigned = masterUpdateShardTask(created, created.doc().assignTo(localNode, false));
        long heldEpoch = assigned.doc().epoch();

        refresh();
        List<Versioned<ShardTaskDoc>> mine = findMyShardTasks();
        assertThat(mine, hasSize(1));
        Versioned<ShardTaskDoc> seen = mine.get(0);
        Versioned<ShardTaskDoc> running = nodeUpdateShardTask(seen, seen.doc().running(randomUUID(), Map.of()), heldEpoch);
        Versioned<ShardTaskDoc> progressed = nodeUpdateShardTask(
            running,
            running.doc().withProgress(Map.of("in_num_docs_received", 5)),
            heldEpoch
        );

        // the master reassigns the task while the node still holds the previous version
        Versioned<ShardTaskDoc> reassigned = masterUpdateShardTask(progressed, progressed.doc().assignTo("another-node", true));
        PlainActionFuture<Versioned<ShardTaskDoc>> staleWrite = new PlainActionFuture<>();
        store.forLocalNode().updateShardTask(progressed, progressed.doc().completed(Map.of()), heldEpoch, staleWrite);
        assertVersionConflict(staleWrite);

        // after re-reading, the node sees it lost the task and the store refuses the write
        Optional<Versioned<ShardTaskDoc>> reread = call(l -> store.forLocalNode().getShardTask(reassigned.doc().id(), l));
        assertThat(reread, equalTo(Optional.of(reassigned)));
        PlainActionFuture<Versioned<ShardTaskDoc>> lostTask = new PlainActionFuture<>();
        store.forLocalNode().updateShardTask(reassigned, reassigned.doc().completed(Map.of()), heldEpoch, lostTask);
        assertRejected(lostTask, "is not assigned to node");

        refresh();
        assertThat(findMyShardTasks(), empty());
    }

    public void testNodeCannotWriteWhenTargetIndexIsGone() {
        String localNode = store.forLocalNode().nodeId();
        if (randomBoolean()) {
            // an index with the same name but another uuid, as after a delete and re-create
            createTargetIndex("target");
        }
        Versioned<ShardTaskDoc> created = createShardTask(newShardTask("target", randomUUID()));
        Versioned<ShardTaskDoc> assigned = masterUpdateShardTask(created, created.doc().assignTo(localNode, false));
        PlainActionFuture<Versioned<ShardTaskDoc>> write = new PlainActionFuture<>();
        store.forLocalNode().updateShardTask(assigned, assigned.doc().running(randomUUID(), Map.of()), assigned.doc().epoch(), write);
        assertRejected(write, "does not exist");
    }

    // ---- typed wrappers, so the generic future helper can infer its type ----

    private Versioned<IndexTaskDoc> createIndexTask(IndexTaskDoc doc) {
        return call(l -> store.forMaster().createIndexTask(doc, l));
    }

    private Versioned<IndexTaskDoc> updateIndexTask(Versioned<IndexTaskDoc> current, IndexTaskDoc next) {
        return call(l -> store.forMaster().updateIndexTask(current, next, l));
    }

    private Optional<Versioned<IndexTaskDoc>> getIndexTask(String targetIndex) {
        return call(l -> store.forMaster().getIndexTask(targetIndex, l));
    }

    private List<Versioned<IndexTaskDoc>> findActiveIndexTasks() {
        return call(l -> store.forMaster().findActiveIndexTasks(l));
    }

    private Versioned<ShardTaskDoc> createShardTask(ShardTaskDoc doc) {
        return call(l -> store.forMaster().createShardTask(doc, l));
    }

    private Versioned<ShardTaskDoc> masterUpdateShardTask(Versioned<ShardTaskDoc> current, ShardTaskDoc next) {
        return call(l -> store.forMaster().updateShardTask(current, next, l));
    }

    private Versioned<ShardTaskDoc> nodeUpdateShardTask(Versioned<ShardTaskDoc> current, ShardTaskDoc next, long heldEpoch) {
        return call(l -> store.forLocalNode().updateShardTask(current, next, heldEpoch, l));
    }

    private List<Versioned<ShardTaskDoc>> findMyShardTasks() {
        return call(l -> store.forLocalNode().findMyShardTasks(l));
    }

    private String createTargetIndex(String name) {
        createIndex(name);
        return getInstanceFromNode(ClusterService.class).state().metadata().getProject().index(name).getIndexUUID();
    }

    private void refresh() {
        client().admin().indices().prepareRefresh(DownsampleStateIndex.INDEX_NAME).get();
    }

    private static <T> T call(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        return future.actionGet(TimeValue.timeValueSeconds(10));
    }

    private static void assertVersionConflict(PlainActionFuture<?> future) {
        Exception e = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(10)));
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(VersionConflictEngineException.class));
    }

    private static void assertRejected(PlainActionFuture<?> future, String expectedMessage) {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> future.actionGet(TimeValue.timeValueSeconds(10)));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }
}
