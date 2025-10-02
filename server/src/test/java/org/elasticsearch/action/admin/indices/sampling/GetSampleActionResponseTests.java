/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

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

public class GetSampleActionResponseTests extends AbstractWireSerializingTestCase<GetSampleAction.Response> {

    @Override
    protected Writeable.Reader<GetSampleAction.Response> instanceReader() {
        return GetSampleAction.Response::new;
    }

    @Override
    protected GetSampleAction.Response createTestInstance() {
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        List<GetSampleAction.NodeResponse> nodes = randomList(
            5,
            () -> new GetSampleAction.NodeResponse(randomNode(), randomList(10, GetSampleActionResponseTests::randomSample))
        );
        return new GetSampleAction.Response(clusterName, nodes, List.of(), randomIntBetween(1, 10));
    }

    @Override
    protected GetSampleAction.Response mutateInstance(GetSampleAction.Response instance) throws IOException {
        ClusterName clusterName = instance.getClusterName();
        List<GetSampleAction.NodeResponse> nodes = new ArrayList<>(instance.getNodes());
        List<FailedNodeException> failures = new ArrayList<>(instance.failures());
        int maxSize = instance.maxSize;
        switch (between(0, 1)) {
            case 0 -> maxSize = randomIntBetween(101, 200);
            case 1 -> {
                DiscoveryNode node = randomNode();
                List<SamplingService.RawDocument> sample = randomList(1, 3, GetSampleActionResponseTests::randomSample);
                nodes.add(new GetSampleAction.NodeResponse(node, sample));
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new GetSampleAction.Response(clusterName, nodes, failures, maxSize);
    }

    private static SamplingService.RawDocument randomSample() {
        return new SamplingService.RawDocument(
            randomIdentifier(),
            randomSource(),
            randomFrom(org.elasticsearch.xcontent.XContentType.values())
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

    private static byte[] randomSource() {
        return randomByteArrayOfLength(randomIntBetween(0, 1000));
    }
}
