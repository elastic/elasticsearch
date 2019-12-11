/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.LocalStateCcr;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ShardChangesTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.of(LocalStateCcr.class, InternalSettingsPlugin.class).collect(Collectors.toList());
    }

    // this emulates what the CCR persistent task will do for pulling
    public void testGetOperationsBasedOnGlobalSequenceId() throws Exception {
        client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.soft_deletes.enabled", true))
            .get();

        client().prepareIndex("index").setId("1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index").setId("2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index").setId("3").setSource("{}", XContentType.JSON).get();

        ShardStats shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        long globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(2L));

        String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
        ShardChangesAction.Request request =  new ShardChangesAction.Request(shardStats.getShardRouting().shardId(), historyUUID);
        request.setFromSeqNo(0L);
        request.setMaxOperationCount(3);
        ShardChangesAction.Response response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        Translog.Index operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(0L));
        assertThat(operation.id(), equalTo("1"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(1L));
        assertThat(operation.id(), equalTo("2"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(2L));
        assertThat(operation.id(), equalTo("3"));

        client().prepareIndex("index").setId("3").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index").setId("4").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index").setId("5").setSource("{}", XContentType.JSON).get();

        shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(5L));

        request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId(), historyUUID);
        request.setFromSeqNo(3L);
        request.setMaxOperationCount(3);
        response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(3L));
        assertThat(operation.id(), equalTo("3"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(4L));
        assertThat(operation.id(), equalTo("4"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(5L));
        assertThat(operation.id(), equalTo("5"));
    }

    public void testMissingOperations() throws Exception {
        client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .put("index.soft_deletes.retention.operations", 0)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "200ms"))
            .get();

        for (int i = 0; i < 32; i++) {
            client().prepareIndex("index").setId("1").setSource("{}", XContentType.JSON).get();
            client().prepareDelete("index", "1").get();
            client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        }
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();
        assertBusy(() -> {
            final ShardStats[] shardsStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards();
            for (final ShardStats shardStats : shardsStats) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertTrue(shardStats.getRetentionLeaseStats().retentionLeases().leases().stream()
                    .allMatch(retentionLease -> retentionLease.retainingSequenceNumber() == maxSeqNo + 1));
            }
        });

        ForceMergeRequest forceMergeRequest = new ForceMergeRequest("index");
        forceMergeRequest.maxNumSegments(1);
        client().admin().indices().forceMerge(forceMergeRequest).actionGet();

        client().admin().indices().execute(RetentionLeaseActions.Add.INSTANCE, new RetentionLeaseActions.AddRequest(
            new ShardId(resolveIndex("index"), 0), "test", RetentionLeaseActions.RETAIN_ALL, "ccr")).get();

        ShardStats shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
        Collection<RetentionLease> retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases().leases();
        ShardChangesAction.Request request =  new ShardChangesAction.Request(shardStats.getShardRouting().shardId(), historyUUID);
        request.setFromSeqNo(0L);
        request.setMaxOperationCount(1);

        {
            ResourceNotFoundException e =
                expectThrows(ResourceNotFoundException.class, () -> client().execute(ShardChangesAction.INSTANCE, request).actionGet());
            assertThat(e.getMessage(), equalTo("Operations are no longer available for replicating. " +
                "Existing retention leases [" + retentionLeases + "]; maybe increase the retention lease period setting " +
                "[index.soft_deletes.retention_lease.period]?"));

            assertThat(e.getMetadataKeys().size(), equalTo(1));
            assertThat(e.getMetadata(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY), notNullValue());
            assertThat(e.getMetadata(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY), contains("0", "0"));
        }
        {
            AtomicReference<Exception> holder = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            client().execute(ShardChangesAction.INSTANCE, request,
                new LatchedActionListener<>(ActionListener.wrap(r -> fail("expected an exception"), holder::set), latch));
            latch.await();

            ElasticsearchException e = (ElasticsearchException) holder.get();
            assertThat(e, notNullValue());
            assertThat(e.getMetadataKeys().size(), equalTo(0));

            ResourceNotFoundException cause = (ResourceNotFoundException) e.getCause();
            assertThat(cause.getMessage(), equalTo("Operations are no longer available for replicating. " +
                "Existing retention leases [" + retentionLeases + "]; maybe increase the retention lease period setting " +
                "[index.soft_deletes.retention_lease.period]?"));
            assertThat(cause.getMetadataKeys().size(), equalTo(1));
            assertThat(cause.getMetadata(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY), notNullValue());
            assertThat(cause.getMetadata(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY), contains("0", "0"));
        }
    }

}
