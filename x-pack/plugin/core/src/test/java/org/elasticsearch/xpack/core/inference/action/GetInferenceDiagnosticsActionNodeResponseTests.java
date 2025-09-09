/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class GetInferenceDiagnosticsActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.NodeResponse> {
    public static GetInferenceDiagnosticsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var randomPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());

        return new GetInferenceDiagnosticsAction.NodeResponse(node, randomPoolStats, randomCacheStats());
    }

    @Override
    protected Writeable.Reader<GetInferenceDiagnosticsAction.NodeResponse> instanceReader() {
        return GetInferenceDiagnosticsAction.NodeResponse::new;
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstance(GetInferenceDiagnosticsAction.NodeResponse instance)
        throws IOException {
        var select = randomIntBetween(0, 3);
        var connPoolStats = instance.getConnectionPoolStats();

        return switch (select) {
            case 0 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    randomInt(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                ),
                randomCacheStats()
            );
            case 1 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    randomInt(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                ),
                randomCacheStats()
            );
            case 2 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    randomInt(),
                    connPoolStats.getMaxConnections()
                ),
                randomCacheStats()
            );
            case 3 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    randomInt()
                ),
                randomCacheStats()
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }

    private static Cache.Stats randomCacheStats() {
        return new Cache.Stats(randomLong(), randomLong(), randomLong());
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstanceForVersion(
        GetInferenceDiagnosticsAction.NodeResponse instance,
        TransportVersion version
    ) {
        return mutateNodeResponseForVersion(instance, version);
    }

    public static GetInferenceDiagnosticsAction.NodeResponse mutateNodeResponseForVersion(
        GetInferenceDiagnosticsAction.NodeResponse instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersions.ML_INFERENCE_ENDPOINT_CACHE)) {
            return new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    instance.getConnectionPoolStats().getLeasedConnections(),
                    instance.getConnectionPoolStats().getPendingConnections(),
                    instance.getConnectionPoolStats().getAvailableConnections(),
                    instance.getConnectionPoolStats().getMaxConnections()
                ),
                null
            );
        } else {
            return instance;
        }
    }
}
