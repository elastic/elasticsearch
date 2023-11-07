/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.template.RolloverEnabledTestTemplateRegistry.TEST_INDEX_PATTERN;
import static org.elasticsearch.xpack.core.template.RolloverEnabledTestTemplateRegistry.TEST_INDEX_TEMPLATE_ID;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class IndexTemplateRegistryRolloverIT extends ESIntegTestCase {

    private ClusterService clusterService;
    private Client client;
    private RolloverEnabledTestTemplateRegistry registry;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @Before
    public void setup() {
        clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        client = client();
        registry = new RolloverEnabledTestTemplateRegistry(
            clusterService.getSettings(),
            clusterService,
            clusterService.threadPool(),
            client,
            xContentRegistry(),
            3L
        );
        registry.initialize();
        ensureGreen();
    }

    public void testRollover() throws Exception {
        ClusterState state = clusterService.state();
        registry.clusterChanged(new ClusterChangedEvent(IndexTemplateRegistryRolloverIT.class.getName(), state, state));
        assertBusy(() -> { assertTrue(clusterService.state().metadata().templatesV2().containsKey(TEST_INDEX_TEMPLATE_ID)); });
        String dsName = TEST_INDEX_PATTERN.replace('*', '1');
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dsName);
        AcknowledgedResponse acknowledgedResponse = client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        assertTrue(acknowledgedResponse.isAcknowledged());
        assertNumberOfBackingIndices(1);
        registry.incrementVersion();
        registry.clusterChanged(new ClusterChangedEvent(IndexTemplateRegistryRolloverIT.class.getName(), clusterService.state(), state));
        assertBusy(() -> assertNumberOfBackingIndices(2));
    }

    private void assertNumberOfBackingIndices(final int expected) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { TEST_INDEX_PATTERN });
        GetDataStreamAction.Response getDataStreamResponse = client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreams = getDataStreamResponse.getDataStreams();
        assertThat(dataStreams, hasSize(1));
        DataStream dataStream = dataStreams.get(0).getDataStream();
        assertThat(dataStream.getIndices(), hasSize(expected));
        assertThat(dataStream.getWriteIndex().getName(), endsWith(String.valueOf(expected)));
    }
}
