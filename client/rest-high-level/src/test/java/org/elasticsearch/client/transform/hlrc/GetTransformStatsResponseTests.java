/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.hlrc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.transforms.hlrc.TransformCheckpointingInfoTests;
import org.elasticsearch.client.transform.transforms.hlrc.TransformIndexerStatsTests;
import org.elasticsearch.client.transform.transforms.hlrc.TransformStatsTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.NodeAttributes;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GetTransformStatsResponseTests extends AbstractResponseTestCase<
    GetTransformStatsAction.Response,
    org.elasticsearch.client.transform.GetTransformStatsResponse> {

    private static NodeAttributes randomNodeAttributes() {
        return new NodeAttributes(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? Collections.emptyMap() : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10))
        );
    }

    private static TransformStats randomTransformStats() {
        return new TransformStats(
            randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomTransformCheckpointingInfo()
        );
    }

    public static Response randomStatsResponse() {
        List<TransformStats> stats = new ArrayList<>();
        int totalStats = randomInt(10);
        for (int i = 0; i < totalStats; ++i) {
            stats.add(randomTransformStats());
        }
        int totalErrors = randomInt(10);
        List<TaskOperationFailure> taskFailures = new ArrayList<>(totalErrors);
        List<ElasticsearchException> nodeFailures = new ArrayList<>(totalErrors);
        for (int i = 0; i < totalErrors; i++) {
            taskFailures.add(new TaskOperationFailure("node1", randomLongBetween(1, 10), new Exception("error")));
            nodeFailures.add(new FailedNodeException("node1", "message", new Exception("error")));
        }
        return new Response(stats, randomLongBetween(stats.size(), 10_000_000L), taskFailures, nodeFailures);
    }

    @Override
    protected Response createServerTestInstance(XContentType xContentType) {
        return randomStatsResponse();
    }

    @Override
    protected GetTransformStatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.transform.GetTransformStatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(Response serverTestInstance, GetTransformStatsResponse clientInstance) {
        assertEquals(serverTestInstance.getTransformsStats().size(), clientInstance.getTransformsStats().size());
        Iterator<TransformStats> serverIt = serverTestInstance.getTransformsStats().iterator();
        Iterator<org.elasticsearch.client.transform.transforms.TransformStats> clientIt = clientInstance.getTransformsStats().iterator();

        while (serverIt.hasNext()) {
            TransformStatsTests.assertHlrcEquals(serverIt.next(), clientIt.next());
        }
        assertThat(serverTestInstance.getCount(), equalTo(clientInstance.getCount()));
    }
}
