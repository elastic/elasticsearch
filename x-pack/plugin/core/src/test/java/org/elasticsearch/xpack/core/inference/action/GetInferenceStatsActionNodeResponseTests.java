/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class GetInferenceStatsActionNodeResponseTests extends AbstractWireSerializingTestCase<GetInferenceStatsAction.NodeResponse> {
    public static GetInferenceStatsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var randomPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());

        return new GetInferenceStatsAction.NodeResponse(node, randomPoolStats);
    }

    @Override
    protected Writeable.Reader<GetInferenceStatsAction.NodeResponse> instanceReader() {
        return GetInferenceStatsAction.NodeResponse::new;
    }

    @Override
    protected GetInferenceStatsAction.NodeResponse createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInferenceStatsAction.NodeResponse mutateInstance(GetInferenceStatsAction.NodeResponse instance) throws IOException {
        var select = randomIntBetween(0, 3);
        var connPoolStats = instance.getConnectionPoolStats();

        return switch (select) {
            case 0 -> new GetInferenceStatsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    randomInt(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 1 -> new GetInferenceStatsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    randomInt(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 2 -> new GetInferenceStatsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    randomInt(),
                    connPoolStats.getMaxConnections()
                )
            );
            case 3 -> new GetInferenceStatsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    randomInt()
                )
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }
}
