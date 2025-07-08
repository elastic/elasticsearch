/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class PipelineConfigurationSyncIT extends ESIntegTestCase {

    public void testAllNodesGetPipelineTrackingClusterState() throws Exception {
        final String pipelineId = "_id";
        GetPipelineResponse getResponse = getPipelines(pipelineId);
        assertFalse(getResponse.isFound());

        final long timeBeforePut = System.currentTimeMillis();
        putJsonPipeline(
            "_id",
            (builder, params) -> builder.field("description", "my_pipeline").startArray("processors").startObject().endObject().endArray()
        );
        final Long timeAfterPut = System.currentTimeMillis();

        getResponse = getPipelines(pipelineId);
        assertTrue(getResponse.isFound());

        final Pipeline node1Pipeline = internalCluster().getInstance(NodeService.class, "node_s0")
            .getIngestService()
            .getPipeline(Metadata.DEFAULT_PROJECT_ID, "_id");
        final Pipeline node2Pipeline = internalCluster().getInstance(NodeService.class, "node_s1")
            .getIngestService()
            .getPipeline(Metadata.DEFAULT_PROJECT_ID, "_id");
        final Pipeline node3Pipeline = internalCluster().getInstance(NodeService.class, "node_s2")
            .getIngestService()
            .getPipeline(Metadata.DEFAULT_PROJECT_ID, "_id");

        assertNotSame(node1Pipeline, node2Pipeline);
        assertNotSame(node2Pipeline, node3Pipeline);

        assertThat(node1Pipeline.getDescription(), equalTo(node2Pipeline.getDescription()));
        assertThat(node2Pipeline.getDescription(), equalTo(node3Pipeline.getDescription()));

        // created_date
        final long node1CreatedDate = node1Pipeline.getCreatedDate().orElseThrow();
        final long node2CreatedDate = node2Pipeline.getCreatedDate().orElseThrow();
        final long node3CreatedDate = node3Pipeline.getCreatedDate().orElseThrow();

        assertThat(node1CreatedDate, equalTo(node2CreatedDate));
        assertThat(node2CreatedDate, equalTo(node3CreatedDate));

        assertThat(node1CreatedDate, greaterThanOrEqualTo(timeBeforePut));
        assertThat(node1CreatedDate, lessThanOrEqualTo(timeAfterPut));

        // modified_date
        final long node1ModifiedDate = node1Pipeline.getModifiedDate().orElseThrow();
        final long node2ModifiedDate = node2Pipeline.getModifiedDate().orElseThrow();
        final long node3ModifiedDate = node3Pipeline.getModifiedDate().orElseThrow();

        assertThat(node1ModifiedDate, equalTo(node2ModifiedDate));
        assertThat(node2ModifiedDate, equalTo(node3ModifiedDate));

        assertThat(node1ModifiedDate, greaterThanOrEqualTo(timeBeforePut));
        assertThat(node1ModifiedDate, lessThanOrEqualTo(timeAfterPut));
    }
}
