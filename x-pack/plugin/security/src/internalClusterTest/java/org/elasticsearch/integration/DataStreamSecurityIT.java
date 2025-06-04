/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamSecurityIT extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSecurity.class, Netty4Plugin.class, MapperExtrasPlugin.class, DataStreamsPlugin.class, Wildcard.class);
    }

    public void testRemoveGhostReference() throws Exception {
        var headers = Map.of(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        final var client = client().filterWithHeader(headers);

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs-*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());

        String dataStreamName = "logs-es";
        var request = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client.execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        assertAcked(client.admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

        var indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(2));

        ClusterState before = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(before.getMetadata().getProject().dataStreams().get(dataStreamName).getIndices(), hasSize(2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DataStream> brokenDataStreamHolder = new AtomicReference<>();
        boolean shouldBreakIndexName = randomBoolean();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask(getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DataStream original = currentState.getMetadata().getProject().dataStreams().get(dataStreamName);
                    String brokenIndexName = shouldBreakIndexName
                        ? original.getIndices().get(0).getName() + "-broken"
                        : original.getIndices().get(0).getName();
                    DataStream broken = original.copy()
                        .setBackingIndices(
                            original.getDataComponent()
                                .copy()
                                .setIndices(List.of(new Index(brokenIndexName, "broken"), original.getIndices().get(1)))
                                .build()
                        )
                        .build();
                    brokenDataStreamHolder.set(broken);
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.getMetadata()).put(broken).build())
                        .build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("error while adding a broken data stream", e);
                    latch.countDown();
                }
            });
        latch.await();
        var ghostReference = brokenDataStreamHolder.get().getIndices().get(0);

        // Many APIs fail with NPE, because of broken data stream:
        var expectedExceptionClass = shouldBreakIndexName ? ElasticsearchSecurityException.class : NullPointerException.class;
        expectThrows(expectedExceptionClass, () -> client.admin().indices().stats(new IndicesStatsRequest()).actionGet());
        expectThrows(expectedExceptionClass, () -> client.search(new SearchRequest()).actionGet());

        assertAcked(
            client.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.removeBackingIndex(dataStreamName, ghostReference.getName()))
                )
            )
        );
        ClusterState after = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(after.getMetadata().getProject().dataStreams().get(dataStreamName).getIndices(), hasSize(1));

        // Data stream resolves now to one backing index.
        // Note, that old backing index still exists, but it is still hidden.
        // The modify data stream api only fixed the data stream by removing a broken reference to a backing index.
        indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(shouldBreakIndexName ? 1 : 2));
    }

}
