/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransformStartWaitForCompletionIT extends TransformSingleNodeTestCase {

    private static final String SOURCE_INDEX = "start-wait-for-completion-source";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    public void testNonBlockingStartDefersDestIndexCreation() throws Exception {
        String transformId = "start-non-blocking";
        String destIndex = transformId + "-dest";
        createSourceIndexWithMapping();
        indexDoc();
        createTransform(transformId, destIndex);

        // The destination index does not exist yet; a non-blocking start must not create it synchronously.
        assertFalse(destinationIndexExists(destIndex));

        var response = client().execute(
            StartTransformAction.INSTANCE,
            new StartTransformAction.Request(transformId, null, null, TimeValue.THIRTY_SECONDS, false)
        ).actionGet(TimeValue.THIRTY_SECONDS);
        assertTrue(response.isAcknowledged());

        // The running task performs the deferred validation and destination-index creation.
        assertBusy(() -> assertTrue(destinationIndexExists(destIndex)), 30, TimeUnit.SECONDS);

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    public void testBlockingStartCreatesDestIndexBeforeReturning() throws Exception {
        String transformId = "start-blocking";
        String destIndex = transformId + "-dest";
        createSourceIndexWithMapping();
        indexDoc();
        createTransform(transformId, destIndex);

        assertFalse(destinationIndexExists(destIndex));

        // A blocking start (the default) validates and creates the destination index before it returns.
        var response = client().execute(
            StartTransformAction.INSTANCE,
            new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS)
        ).actionGet(TimeValue.THIRTY_SECONDS);
        assertTrue(response.isAcknowledged());
        assertTrue(destinationIndexExists(destIndex));

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    public void testNonBlockingStartFailsFastOnInvalidDestination() throws Exception {
        String transformId = "start-non-blocking-bad-dest";
        String destName = "nb-bad-dest";
        createSourceIndexWithMapping();
        indexDoc();
        // Valid at creation time: destName does not resolve to any index yet.
        createTransform(transformId, destName);

        // Drift after creation: point destName at two indices with no designated write index, so it no longer
        // resolves to a single writable destination and the cheap synchronous validation must reject it.
        indicesAdmin().create(new CreateIndexRequest(destName + "-1").alias(new Alias(destName))).actionGet();
        indicesAdmin().create(new CreateIndexRequest(destName + "-2").alias(new Alias(destName))).actionGet();

        // A non-blocking start still runs the cheap validation synchronously, so an obviously-broken destination
        // fails fast with an error rather than acknowledging and surfacing later as a FAILED transform.
        var e = expectThrows(
            Exception.class,
            () -> client().execute(
                StartTransformAction.INSTANCE,
                new StartTransformAction.Request(transformId, null, null, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS)
        );
        assertThat(e.getMessage(), containsString(destName));

        // The task must not have been committed: the transform is still stopped.
        var stats = client().execute(
            GetTransformStatsAction.INSTANCE,
            new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, true)
        ).actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(stats.getTransformsStats().get(0).getState(), equalTo(TransformStats.State.STOPPED));

        deleteTransform(transformId);
    }

    private void createTransform(String transformId, String destIndex) {
        createTransform(
            TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
                .setDest(new DestConfig(destIndex, null, null))
                .setFrequency(TimeValue.ONE_MINUTE)
                .setLatestConfig(new LatestConfig(List.of("key"), "time"))
                .build()
        );
    }

    private boolean destinationIndexExists(String index) {
        var clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return clusterState.metadata().getProject().hasIndexAbstraction(index);
    }

    private void createSourceIndexWithMapping() throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("properties");
            builder.startObject("time").field("type", "date").endObject();
            builder.startObject("key").field("type", "keyword").endObject();
            builder.endObject();
            builder.endObject();
            indicesAdmin().create(new CreateIndexRequest(SOURCE_INDEX).mapping(builder)).actionGet();
        }
    }

    private void indexDoc() {
        client().bulk(
            new BulkRequest().add(
                new IndexRequest(SOURCE_INDEX).source(Map.of("time", Instant.now().toEpochMilli(), "key", randomAlphaOfLength(5)))
            )
        ).actionGet(TimeValue.THIRTY_SECONDS);
    }
}
