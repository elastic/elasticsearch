/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction.Response.MlMemoryStats;

import java.net.InetAddress;

public class MlMemoryStatsTests extends AbstractWireSerializingTestCase<MlMemoryStats> {

    @Override
    protected Writeable.Reader<MlMemoryStats> instanceReader() {
        return MlMemoryStats::new;
    }

    @Override
    protected MlMemoryStats createTestInstance() {
        DiscoveryNode node = DiscoveryNodeUtils.create(
            randomAlphaOfLength(20),
            new TransportAddress(InetAddress.getLoopbackAddress(), randomIntBetween(1024, 65535))
        );
        return createTestInstance(node);
    }

    @Override
    protected MlMemoryStats mutateInstance(MlMemoryStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static MlMemoryStats createTestInstance(DiscoveryNode node) {
        return new MlMemoryStats(
            node,
            ByteSizeValue.ofGb(randomLongBetween(1, 64)),
            ByteSizeValue.ofGb(randomLongBetween(1, 64)),
            ByteSizeValue.ofGb(randomLongBetween(0, 48)),
            ByteSizeValue.ofMb(randomLongBetween(0, 20000)),
            ByteSizeValue.ofMb(randomLongBetween(0, 20000)),
            ByteSizeValue.ofMb(randomLongBetween(0, 20000)),
            ByteSizeValue.ofKb(randomLongBetween(0, 30000)),
            ByteSizeValue.ofGb(randomLongBetween(0, 32)),
            ByteSizeValue.ofGb(randomLongBetween(0, 16)),
            ByteSizeValue.ofMb(randomLongBetween(0, 10000))
        );
    }
}
