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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class GetInferenceDiagnosticsActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.NodeResponse> {
    public static GetInferenceDiagnosticsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var randomExternalPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());
        var randomEisPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());

        return new GetInferenceDiagnosticsAction.NodeResponse(node, randomExternalPoolStats, randomEisPoolStats);
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
        var connPoolStats = instance.getExternalConnectionPoolStats();
        var eisPoolStats = instance.getEisMtlsConnectionPoolStats();

        return switch (select) {
            case 0 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    randomInt(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                ),
                new PoolStats(
                    randomInt(),
                    eisPoolStats.getPendingConnections(),
                    eisPoolStats.getAvailableConnections(),
                    eisPoolStats.getMaxConnections()
                )
            );
            case 1 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    randomInt(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                ),
                new PoolStats(
                    eisPoolStats.getLeasedConnections(),
                    randomInt(),
                    eisPoolStats.getAvailableConnections(),
                    eisPoolStats.getMaxConnections()
                )
            );
            case 2 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    randomInt(),
                    connPoolStats.getMaxConnections()
                ),
                new PoolStats(
                    eisPoolStats.getLeasedConnections(),
                    eisPoolStats.getPendingConnections(),
                    randomInt(),
                    eisPoolStats.getMaxConnections()
                )
            );
            case 3 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    randomInt()
                ),
                new PoolStats(
                    eisPoolStats.getLeasedConnections(),
                    eisPoolStats.getPendingConnections(),
                    eisPoolStats.getAvailableConnections(),
                    randomInt()
                )
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstanceForVersion(
        GetInferenceDiagnosticsAction.NodeResponse instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersions.INFERENCE_API_EIS_DIAGNOSTICS)) {
            return new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    instance.getExternalConnectionPoolStats().getLeasedConnections(),
                    instance.getExternalConnectionPoolStats().getPendingConnections(),
                    instance.getExternalConnectionPoolStats().getAvailableConnections(),
                    instance.getExternalConnectionPoolStats().getMaxConnections()
                ),
                new PoolStats(0, 0, 0, 0)
            );
        } else {
            return instance;
        }
    }
}
