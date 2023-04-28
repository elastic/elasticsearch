/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.net.InetAddress;

public class TrainedModelCacheInfoRequestTests extends AbstractWireSerializingTestCase<TrainedModelCacheInfoAction.Request> {

    @Override
    protected Writeable.Reader<TrainedModelCacheInfoAction.Request> instanceReader() {
        return TrainedModelCacheInfoAction.Request::new;
    }

    @Override
    protected TrainedModelCacheInfoAction.Request createTestInstance() {
        int numNodes = randomIntBetween(1, 20);
        DiscoveryNode[] nodes = new DiscoveryNode[numNodes];
        for (int i = 0; i < numNodes; ++i) {
            nodes[i] = new DiscoveryNode(
                randomAlphaOfLength(20),
                new TransportAddress(InetAddress.getLoopbackAddress(), 9200 + i),
                Version.CURRENT
            );
        }
        return new TrainedModelCacheInfoAction.Request(nodes);
    }

    @Override
    protected TrainedModelCacheInfoAction.Request mutateInstance(TrainedModelCacheInfoAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
