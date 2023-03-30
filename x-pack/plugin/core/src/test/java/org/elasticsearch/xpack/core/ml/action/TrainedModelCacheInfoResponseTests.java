/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction.Response.CacheInfo;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class TrainedModelCacheInfoResponseTests extends AbstractWireSerializingTestCase<TrainedModelCacheInfoAction.Response> {

    @Override
    protected Writeable.Reader<TrainedModelCacheInfoAction.Response> instanceReader() {
        return TrainedModelCacheInfoAction.Response::new;
    }

    @Override
    protected TrainedModelCacheInfoAction.Response createTestInstance() {
        int numNodes = randomIntBetween(1, 20);
        List<CacheInfo> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; ++i) {
            DiscoveryNode node = new DiscoveryNode(
                randomAlphaOfLength(20),
                new TransportAddress(InetAddress.getLoopbackAddress(), 9200 + i),
                Version.CURRENT
            );
            nodes.add(CacheInfoTests.createTestInstance(node));
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
        return new TrainedModelCacheInfoAction.Response(ClusterName.DEFAULT, nodes, failures);
    }

    @Override
    protected TrainedModelCacheInfoAction.Response mutateInstance(TrainedModelCacheInfoAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
