/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class IngestProcessorNotInstalledOnAllNodesIT extends ESIntegTestCase {

    private final BytesReference pipelineSource;
    private volatile boolean installPlugin;

    public IngestProcessorNotInstalledOnAllNodesIT() throws IOException {
        pipelineSource = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return installPlugin ? Arrays.asList(IngestTestPlugin.class) : Collections.emptyList();
    }

    public void testFailPipelineCreation() {
        installPlugin = true;
        String node1 = internalCluster().startNode();
        installPlugin = false;
        String node2 = internalCluster().startNode();
        ensureStableCluster(2, node1);
        ensureStableCluster(2, node2);

        assertThat(
            safeAwaitAndUnwrapFailure(
                ElasticsearchParseException.class,
                AcknowledgedResponse.class,
                l -> client().execute(
                    PutPipelineTransportAction.TYPE,
                    IngestPipelineTestUtils.putJsonPipelineRequest("id", pipelineSource),
                    l
                )
            ).getMessage(),
            containsString("Processor type [test] is not installed on node")
        );
    }

    public void testFailPipelineCreationProcessorNotInstalledOnMasterNode() throws Exception {
        internalCluster().startNode();
        installPlugin = true;
        internalCluster().startNode();

        assertThat(
            safeAwaitAndUnwrapFailure(
                ElasticsearchParseException.class,
                AcknowledgedResponse.class,
                l -> client().execute(
                    PutPipelineTransportAction.TYPE,
                    IngestPipelineTestUtils.putJsonPipelineRequest("id", pipelineSource),
                    l
                )
            ).getMessage(),
            equalTo("No processor type exists with name [test]")
        );
    }

    // If there is pipeline defined and a node joins that doesn't have the processor installed then
    // that pipeline can't be used on this node.
    public void testFailStartNode() throws Exception {
        installPlugin = true;
        String node1 = internalCluster().startNode();

        putJsonPipeline("_id", pipelineSource);
        Pipeline pipeline = internalCluster().getInstance(NodeService.class, node1)
            .getIngestService()
            .getPipeline(Metadata.DEFAULT_PROJECT_ID, "_id");
        assertThat(pipeline, notNullValue());

        installPlugin = false;
        String node2 = internalCluster().startNode();
        pipeline = internalCluster().getInstance(NodeService.class, node2)
            .getIngestService()
            .getPipeline(Metadata.DEFAULT_PROJECT_ID, "_id");

        assertNotNull(pipeline);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(
            pipeline.getDescription(),
            equalTo("this is a place holder pipeline, because pipeline with id [_id] could not be loaded")
        );
    }

}
