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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction.Response.CacheInfo;

import java.net.InetAddress;

public class CacheInfoTests extends AbstractWireSerializingTestCase<CacheInfo> {

    @Override
    protected Writeable.Reader<CacheInfo> instanceReader() {
        return CacheInfo::new;
    }

    @Override
    protected CacheInfo createTestInstance() {
        DiscoveryNode node = new DiscoveryNode(
            randomAlphaOfLength(20),
            new TransportAddress(InetAddress.getLoopbackAddress(), randomIntBetween(1024, 65535)),
            Version.CURRENT
        );
        return createTestInstance(node);
    }

    @Override
    protected CacheInfo mutateInstance(CacheInfo instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static CacheInfo createTestInstance(DiscoveryNode node) {
        return new CacheInfo(node, ByteSizeValue.ofMb(randomLongBetween(1000, 30000)), ByteSizeValue.ofMb(randomLongBetween(0, 1000)));
    }
}
