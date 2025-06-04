/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class MlMemoryActionResponseTests extends AbstractWireSerializingTestCase<MlMemoryAction.Response> {

    @Override
    protected Writeable.Reader<MlMemoryAction.Response> instanceReader() {
        return MlMemoryAction.Response::new;
    }

    @Override
    protected MlMemoryAction.Response createTestInstance() {
        int numNodes = randomIntBetween(1, 20);
        List<MlMemoryAction.Response.MlMemoryStats> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; ++i) {
            DiscoveryNode node = DiscoveryNodeUtils.create(
                randomAlphaOfLength(20),
                new TransportAddress(InetAddress.getLoopbackAddress(), 9200 + i)
            );
            nodes.add(MlMemoryStatsTests.createTestInstance(node));
        }
        int numFailures = randomIntBetween(0, 5);
        List<FailedNodeException> failures = (numFailures > 0) ? new ArrayList<>(numFailures) : List.of();
        for (int i = 0; i < numFailures; ++i) {
            failures.add(
                new FailedNodeException(
                    randomAlphaOfLength(20),
                    randomAlphaOfLength(50),
                    new ElasticsearchException(randomAlphaOfLength(30))
                )
            );
        }
        return new MlMemoryAction.Response(ClusterName.DEFAULT, nodes, failures);
    }

    @Override
    protected MlMemoryAction.Response mutateInstance(MlMemoryAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
