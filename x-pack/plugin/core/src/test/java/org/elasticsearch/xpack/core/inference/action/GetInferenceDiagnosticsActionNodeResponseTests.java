/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class GetInferenceDiagnosticsActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.NodeResponse> {

    private static final TransportVersion ML_INFERENCE_ENDPOINT_CACHE = TransportVersion.fromName("ml_inference_endpoint_cache");
    private static final TransportVersion INFERENCE_API_EIS_DIAGNOSTICS = TransportVersion.fromName("inference_api_eis_diagnostics");

    public static GetInferenceDiagnosticsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var randomExternalPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());
        var randomEisPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());

        return new GetInferenceDiagnosticsAction.NodeResponse(node, randomExternalPoolStats, randomEisPoolStats, randomCacheStats());
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
        if (randomBoolean()) {
            PoolStats mutatedConnPoolStats = mutatePoolStats(instance.getExternalConnectionPoolStats());
            PoolStats eisPoolStats = copyPoolStats(instance.getEisMtlsConnectionPoolStats());
            return new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                mutatedConnPoolStats,
                eisPoolStats,
                randomCacheStats()
            );
        } else {
            PoolStats connPoolStats = copyPoolStats(instance.getExternalConnectionPoolStats());
            PoolStats mutatedEisPoolStats = mutatePoolStats(instance.getEisMtlsConnectionPoolStats());
            return new GetInferenceDiagnosticsAction.NodeResponse(instance.getNode(), connPoolStats, mutatedEisPoolStats, null);
        }
    }

    private PoolStats mutatePoolStats(GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats stats)
        throws UnsupportedEncodingException {
        var select = randomIntBetween(0, 3);
        return switch (select) {
            case 0 -> new PoolStats(randomInt(), stats.getPendingConnections(), stats.getAvailableConnections(), stats.getMaxConnections());
            case 1 -> new PoolStats(stats.getLeasedConnections(), randomInt(), stats.getAvailableConnections(), stats.getMaxConnections());
            case 2 -> new PoolStats(stats.getLeasedConnections(), stats.getPendingConnections(), randomInt(), stats.getMaxConnections());
            case 3 -> new PoolStats(
                stats.getLeasedConnections(),
                stats.getPendingConnections(),
                stats.getAvailableConnections(),
                randomInt()
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }

    private PoolStats copyPoolStats(GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats stats) {
        return new PoolStats(
            stats.getLeasedConnections(),
            stats.getPendingConnections(),
            stats.getAvailableConnections(),
            stats.getMaxConnections()
        );
    }

    private static GetInferenceDiagnosticsAction.NodeResponse.Stats randomCacheStats() {
        return new GetInferenceDiagnosticsAction.NodeResponse.Stats(
            randomInt(),
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE)
        );
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
        if (version.supports(ML_INFERENCE_ENDPOINT_CACHE)) {
            return instance;
        }

        var eisMltsConnectionPoolStats = version.supports(INFERENCE_API_EIS_DIAGNOSTICS)
            ? new PoolStats(
                instance.getEisMtlsConnectionPoolStats().getLeasedConnections(),
                instance.getEisMtlsConnectionPoolStats().getPendingConnections(),
                instance.getEisMtlsConnectionPoolStats().getAvailableConnections(),
                instance.getEisMtlsConnectionPoolStats().getMaxConnections()
            )
            : new PoolStats(0, 0, 0, 0);

        return new GetInferenceDiagnosticsAction.NodeResponse(
            instance.getNode(),
            new PoolStats(
                instance.getExternalConnectionPoolStats().getLeasedConnections(),
                instance.getExternalConnectionPoolStats().getPendingConnections(),
                instance.getExternalConnectionPoolStats().getAvailableConnections(),
                instance.getExternalConnectionPoolStats().getMaxConnections()
            ),
            eisMltsConnectionPoolStats,
            null
        );
    }
}
