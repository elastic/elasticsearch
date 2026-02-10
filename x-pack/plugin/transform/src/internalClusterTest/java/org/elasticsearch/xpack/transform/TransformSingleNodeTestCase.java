/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.TransportResetFeatureStateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.junit.After;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class TransformSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateTransform.class, ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest, transform").build();
    }

    @After
    public void cleanup() {
        client().execute(TransportResetFeatureStateAction.TYPE, new ResetFeatureStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
    }

    protected <T> void assertAsync(
        Consumer<ActionListener<T>> function,
        T expected,
        CheckedConsumer<T, ? extends Exception> onAnswer,
        Consumer<Exception> onException
    ) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            if (expected == null) {
                fail("expected an exception but got a response");
            } else {
                assertThat(r, equalTo(expected));
            }
            if (onAnswer != null) {
                onAnswer.accept(r);
            }
        }, e -> {
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else {
                onException.accept(e);
            }
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

    protected void createSourceIndex(String index) {
        indicesAdmin().create(new CreateIndexRequest(index)).actionGet();
    }

    protected void indexRandomDiceDoc(String index) {
        client().bulk(
            new BulkRequest().add(new IndexRequest(index).source(Map.of("time", Instant.now().toEpochMilli(), "roll", randomInt(20))))
        ).actionGet(TimeValue.THIRTY_SECONDS);
    }

    protected void createTransform(TransformConfig transformConfig) {
        var request = new PutTransformAction.Request(transformConfig, false, TimeValue.THIRTY_SECONDS);
        client().execute(PutTransformAction.INSTANCE, request).actionGet(TimeValue.THIRTY_SECONDS);
    }

    protected void createDiceTransform(String transformId, String transformSrc, String projectRouting) {
        createTransform(
            TransformConfig.builder()
                .setId(transformId)
                .setDest(new DestConfig(transformId, null, null))
                .setSource(new SourceConfig(new String[] { transformSrc }, QueryConfig.matchAll(), Map.of(), projectRouting))
                .setFrequency(TimeValue.ONE_MINUTE)
                .setSyncConfig(new TimeSyncConfig("time", TimeValue.ONE_MINUTE))
                .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
                .build()
        );
    }

    protected TransformConfig getTransform(String transformId) {
        var response = client().execute(
            GetTransformAction.INSTANCE,
            new GetTransformAction.Request(transformId, false, TimeValue.THIRTY_SECONDS)
        ).actionGet(TimeValue.THIRTY_SECONDS);
        var configs = response.getTransformConfigurations();
        assertThat(configs, hasSize(1));
        return configs.getFirst();
    }

    protected void updateTransform(String transformId, TransformConfigUpdate transformConfigUpdate) {
        var request = new UpdateTransformAction.Request(transformConfigUpdate, transformId, false, TimeValue.THIRTY_SECONDS);
        client().execute(UpdateTransformAction.INSTANCE, request).actionGet(TimeValue.THIRTY_SECONDS);
    }

    protected Set<String> getAuditMessages(String transformId) {
        var searchRequest = new SearchRequest(TransformInternalIndexConstants.AUDIT_INDEX_PATTERN);
        var searchResponse = client().search(searchRequest).actionGet(TimeValue.THIRTY_SECONDS);
        return Arrays.stream(searchResponse.getHits().getHits())
            .map(SearchHit::getSourceAsMap)
            .filter(source -> Objects.equals(source.get("transform_id"), transformId))
            .map(source -> source.get("message"))
            .map(Object::toString)
            .collect(Collectors.toSet());
    }

}
