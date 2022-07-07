/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStream.TimestampField;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamSecurityIT extends SecurityIntegTestCase {

    @After
    public void cleanup() {
        DeleteDataStreamAction.Request deleteDataStreamsRequest = new DeleteDataStreamAction.Request("*");
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest).actionGet());

        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        assertAcked(client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, Netty4Plugin.class, MapperExtrasPlugin.class, DataStreamsPlugin.class);
    }

    public void testRemoveGhostReference() throws Exception {
        Map<String, String> headers = org.elasticsearch.core.Map.of(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        final Client client = client().filterWithHeader(headers);

        PutComposableIndexTemplateAction.Request putTemplateRequest = new PutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            new ComposableIndexTemplate(
                org.elasticsearch.core.List.of("logs-*"),
                null,
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        assertAcked(client.execute(PutComposableIndexTemplateAction.INSTANCE, putTemplateRequest).actionGet());

        String dataStreamName = "logs-es";
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStreamName);
        assertAcked(client.execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        assertAcked(client.admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

        IndicesStatsResponse indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(2));

        ClusterState before = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(before.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DataStream> brokenDataStreamHolder = new AtomicReference<>();
        boolean shouldBreakIndexName = randomBoolean();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitStateUpdateTask(getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DataStream original = currentState.getMetadata().dataStreams().get(dataStreamName);
                    String brokenIndexName = shouldBreakIndexName
                        ? original.getIndices().get(0).getName() + "-broken"
                        : original.getIndices().get(0).getName();
                    DataStream broken = new DataStream(
                        original.getName(),
                        new TimestampField("@timestamp"),
                        org.elasticsearch.core.List.of(new Index(brokenIndexName, "broken"), original.getIndices().get(1)),
                        original.getGeneration(),
                        original.getMetadata(),
                        original.isHidden(),
                        original.isReplicated(),
                        original.isSystem()
                    );
                    brokenDataStreamHolder.set(broken);
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.getMetadata()).put(broken).build())
                        .build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("error while adding a broken data stream", e);
                    latch.countDown();
                }
            });
        latch.await();
        Index ghostReference = brokenDataStreamHolder.get().getIndices().get(0);

        // Many APIs fail with NPE, because of broken data stream:
        if (shouldBreakIndexName) {
            expectThrows(ElasticsearchSecurityException.class, () -> client.admin().indices().stats(new IndicesStatsRequest()).actionGet());
            expectThrows(ElasticsearchSecurityException.class, () -> client.search(new SearchRequest()).actionGet());
        } else {
            expectThrows(NullPointerException.class, () -> client.admin().indices().stats(new IndicesStatsRequest()).actionGet());
            expectThrows(NullPointerException.class, () -> client.search(new SearchRequest()).actionGet());
        }

        assertAcked(
            client.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, ghostReference.getName()))
                )
            ).actionGet()
        );
        ClusterState after = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(after.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(1));

        // Data stream resolves now to one backing index.
        // Note, that old backing index still exists, but it is still hidden.
        // The modify data stream api only fixed the data stream by removing a broken reference to a backing index.
        indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(shouldBreakIndexName ? 1 : 2));
    }

}
