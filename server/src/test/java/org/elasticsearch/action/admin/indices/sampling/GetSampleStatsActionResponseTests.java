/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.Response;

public class GetSampleStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        List<GetSampleStatsAction.NodeResponse> nodes = randomList(
            5,
            () -> new GetSampleStatsAction.NodeResponse(randomNode(), randomStats())
        );
        return new Response(clusterName, nodes, List.of(), randomIntBetween(1, 10));
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        ClusterName clusterName = instance.getClusterName();
        List<GetSampleStatsAction.NodeResponse> nodes = new ArrayList<>(instance.getNodes());
        List<FailedNodeException> failures = new ArrayList<>(instance.failures());
        int maxSize = instance.maxSize;
        switch (between(0, 1)) {
            case 0 -> maxSize = randomIntBetween(101, 200);
            case 1 -> {
                DiscoveryNode node = randomNode();
                nodes.add(new GetSampleStatsAction.NodeResponse(node, randomStats()));
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Response(clusterName, nodes, failures, maxSize);
    }

    private static SamplingService.SampleStats randomStats() {
        return new SamplingService.SampleStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomBoolean() ? null : new ElasticsearchException("fail")
        );
    }

    private static DiscoveryNode randomNode() {
        return new DiscoveryNode(
            randomIdentifier(),
            randomIdentifier(),
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionInformation.CURRENT
        );
    }
}
