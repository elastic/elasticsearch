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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.junit.After;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    protected TransformConfig getTransform(String transformId) {
        var response = client().execute(
            GetTransformAction.INSTANCE,
            new GetTransformAction.Request(transformId, false, TimeValue.THIRTY_SECONDS)
        ).actionGet(TimeValue.THIRTY_SECONDS);
        var configs = response.getTransformConfigurations();
        assertThat(configs, hasSize(1));
        return configs.getFirst();
    }

}
