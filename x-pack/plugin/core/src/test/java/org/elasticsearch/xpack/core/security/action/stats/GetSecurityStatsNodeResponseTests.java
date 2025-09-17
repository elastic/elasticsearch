/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.stats;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Map;

public class GetSecurityStatsNodeResponseTests extends AbstractWireSerializingTestCase<GetSecurityStatsNodeResponse> {

    @Override
    protected Writeable.Reader<GetSecurityStatsNodeResponse> instanceReader() {
        return GetSecurityStatsNodeResponse::new;
    }

    @Override
    protected GetSecurityStatsNodeResponse createTestInstance() {
        return new GetSecurityStatsNodeResponse(
            DiscoveryNodeUtils.create(randomUUID()),
            randomBoolean() ? null : Map.of("key", randomUUID())
        );
    }

    @Override
    protected GetSecurityStatsNodeResponse mutateInstance(GetSecurityStatsNodeResponse instance) throws IOException {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new GetSecurityStatsNodeResponse(DiscoveryNodeUtils.create(randomUUID()), instance.getRolesStoreStats());
            case 1 -> new GetSecurityStatsNodeResponse(instance.getDiscoveryNode(), Map.of("key", randomUUID()));
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
