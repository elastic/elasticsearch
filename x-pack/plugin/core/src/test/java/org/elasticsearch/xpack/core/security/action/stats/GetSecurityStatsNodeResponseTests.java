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
import java.util.Objects;

public class GetSecurityStatsNodeResponseTests extends AbstractWireSerializingTestCase<GetSecurityStatsNodeResponse> {

    @Override
    protected Writeable.Reader<GetSecurityStatsNodeResponse> instanceReader() {
        return GetSecurityStatsNodeResponse::new;
    }

    @Override
    protected GetSecurityStatsNodeResponse createTestInstance() {
        return new GetSecurityStatsNodeResponse(
            DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).ephemeralId(randomAlphanumericOfLength(10)).build(),
            randomBoolean() ? null : Map.of("key", randomAlphaOfLength(5))
        );
    }

    @Override
    protected GetSecurityStatsNodeResponse mutateInstance(GetSecurityStatsNodeResponse instance) throws IOException {
        final var node = instance.getDiscoveryNode();
        final var value = Objects.requireNonNullElse(instance.getRolesStoreStats(), Map.of()).get("key");
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new GetSecurityStatsNodeResponse(
                DiscoveryNodeUtils.builder(randomValueOtherThan(node.getId(), () -> randomAlphaOfLength(10)))
                    // DiscoverNode#hashCode only tests ephemeralId, so make sure to change it too
                    .ephemeralId(randomValueOtherThan(node.getEphemeralId(), () -> randomAlphanumericOfLength(10)))
                    .build(),
                instance.getRolesStoreStats()
            );
            case 1 -> new GetSecurityStatsNodeResponse(node, Map.of("key", randomValueOtherThan(value, () -> randomAlphaOfLength(5))));
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
