/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TransformOpTypeIT extends TransformSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        var plugins = new ArrayList<>(super.getPlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    private void waitForTransformStopped(String transformId) throws Exception {
        assertBusy(() -> {
            var statsResponse = client().execute(
                GetTransformStatsAction.INSTANCE,
                new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS);
            assertThat(statsResponse.getTransformsStats().size(), equalTo(1));
            assertThat(statsResponse.getTransformsStats().get(0).getState(), equalTo(TransformStats.State.STOPPED));
        });
    }

    /**
     * Default op_type (INDEX) auto-creates the destination index if it does not exist.
     */
    public void testDefaultOpTypeIndexCreatesDestIndex() throws Exception {
        String sourceIndex = "test-source-default-op-type";
        String destIndex = "test-dest-default-op-type";
        String transformId = "test-default-op-type";

        createSourceIndex(sourceIndex);
        for (int i = 0; i < 5; i++) {
            indexRandomDiceDoc(sourceIndex);
        }
        client().admin().indices().prepareRefresh(sourceIndex).get();

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(sourceIndex))
            .setDest(new DestConfig(destIndex, null, null))
            .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
            .build();
        createTransform(config);

        client().execute(
            StartTransformAction.INSTANCE,
            new StartTransformAction.Request(transformId, null, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT)
        ).actionGet();

        waitForTransformStopped(transformId);

        client().admin().indices().prepareRefresh(destIndex).get();
        var searchResponse = client().search(new SearchRequest(destIndex)).actionGet(TimeValue.THIRTY_SECONDS);
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
        } finally {
            searchResponse.decRef();
        }

        deleteTransform(transformId);
    }

    /**
     * op_type=create requires the destination index to already exist. When it does, the transform runs normally and
     * writes documents with auto-generated IDs (not deterministic transform-computed IDs).
     */
    public void testOpTypeCreateWithPreExistingDestIndex() throws Exception {
        String sourceIndex = "test-source-op-type-create";
        String destIndex = "test-dest-op-type-create";
        String transformId = "test-op-type-create";

        createSourceIndex(sourceIndex);
        for (int i = 0; i < 5; i++) {
            indexRandomDiceDoc(sourceIndex);
        }
        client().admin().indices().prepareRefresh(sourceIndex).get();

        indicesAdmin().create(new CreateIndexRequest(destIndex)).actionGet();

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(sourceIndex))
            .setDest(new DestConfig(destIndex, null, null, DocWriteRequest.OpType.CREATE))
            .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
            .build();
        createTransform(config);

        client().execute(
            StartTransformAction.INSTANCE,
            new StartTransformAction.Request(transformId, null, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT)
        ).actionGet();

        waitForTransformStopped(transformId);

        client().admin().indices().prepareRefresh(destIndex).get();
        var searchResponse = client().search(new SearchRequest(destIndex)).actionGet(TimeValue.THIRTY_SECONDS);
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
        } finally {
            searchResponse.decRef();
        }

        deleteTransform(transformId);
    }

    /**
     * op_type=create with a non-existent destination index is rejected at creation time because the
     * destination must already exist when op_type is create.
     */
    public void testOpTypeCreateWithMissingDestIndexIsRejected() {
        String sourceIndex = "test-source-op-type-create-missing";
        String destIndex = "test-dest-op-type-create-missing";
        String transformId = "test-op-type-create-missing-dest";

        createSourceIndex(sourceIndex);

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(sourceIndex))
            .setDest(new DestConfig(destIndex, null, null, DocWriteRequest.OpType.CREATE))
            .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
            .build();

        ValidationException e = expectThrows(ValidationException.class, () -> createTransform(config));
        assertThat(e.getMessage(), containsString("does not exist"));
        assertThat(e.getMessage(), containsString("op_type"));
    }

    /**
     * Combining op_type=create with a retention_policy is invalid because the retention policy
     * deletes documents, which contradicts create-only write semantics.
     */
    public void testOpTypeCreateWithRetentionPolicyIsRejected() {
        String sourceIndex = "test-source-op-type-create-retention";
        String destIndex = "test-dest-op-type-create-retention";
        String transformId = "test-op-type-create-retention";

        createSourceIndex(sourceIndex);

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(sourceIndex))
            .setDest(new DestConfig(destIndex, null, null, DocWriteRequest.OpType.CREATE))
            .setSyncConfig(new TimeSyncConfig("time", TimeValue.timeValueSeconds(60)))
            .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
            .setRetentionPolicyConfig(new TimeRetentionPolicyConfig("time", TimeValue.timeValueDays(1)))
            .build();

        ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, () -> createTransform(config));
        assertThat(e.getMessage(), containsString("op_type: create"));
        assertThat(e.getMessage(), containsString("retention_policy"));
    }

    /**
     * op_type=create targeting a pre-existing data stream should write unique, append-only documents.
     * Data streams inherently require op_type=create, so this validates the natural pairing.
     */
    public void testOpTypeCreateWithDataStreamDest() throws Exception {
        String sourceIndex = "test-source-ds-op-type-create";
        String destDataStream = "test-dest-ds-op-type-create";
        String transformId = "test-ds-op-type-create";

        indicesAdmin().prepareCreate(sourceIndex).setMapping("@timestamp", "type=date", "roll", "type=integer").get();

        int numDistinctRolls = 5;
        for (int i = 0; i < numDistinctRolls; i++) {
            client().bulk(
                new BulkRequest().add(
                    new IndexRequest(sourceIndex).source(Map.of("@timestamp", Instant.now().toEpochMilli(), "roll", i + 1))
                )
            ).actionGet(TimeValue.THIRTY_SECONDS);
        }
        client().admin().indices().prepareRefresh(sourceIndex).get();

        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request("ds-template").indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(destDataStream))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
        ).actionGet();

        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, destDataStream)
        ).actionGet();

        try {
            TransformConfig config = TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(sourceIndex))
                .setDest(new DestConfig(destDataStream, null, null, DocWriteRequest.OpType.CREATE))
                .setLatestConfig(new LatestConfig(List.of("roll"), "@timestamp"))
                .build();
            createTransform(config);

            client().execute(
                StartTransformAction.INSTANCE,
                new StartTransformAction.Request(transformId, null, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT)
            ).actionGet();

            waitForTransformStopped(transformId);

            client().admin().indices().prepareRefresh(destDataStream).get();
            var searchResponse = client().search(new SearchRequest(destDataStream)).actionGet(TimeValue.THIRTY_SECONDS);
            try {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDistinctRolls));
            } finally {
                searchResponse.decRef();
            }

            var statsResponse = client().execute(
                GetTransformStatsAction.INSTANCE,
                new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS);
            TransformStats stats = statsResponse.getTransformsStats().get(0);
            assertThat(stats.getIndexerStats().getOutputDocuments(), equalTo((long) numDistinctRolls));
            assertThat(stats.getIndexerStats().getNumInvocations(), greaterThan(0L));

            deleteTransform(transformId);
        } finally {
            client().execute(
                DeleteDataStreamAction.INSTANCE,
                new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { destDataStream })
            ).actionGet();
        }
    }
}
