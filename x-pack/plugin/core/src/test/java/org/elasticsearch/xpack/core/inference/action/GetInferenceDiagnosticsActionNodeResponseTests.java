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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.SerializableStats;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class GetInferenceDiagnosticsActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.NodeResponse> {
    public static GetInferenceDiagnosticsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var randomPoolStats = new PoolStats(randomInt(), randomInt(), randomInt(), randomInt());

        return new GetInferenceDiagnosticsAction.NodeResponse(node, randomPoolStats, new TestStats(randomInt()));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return registryWithTestStats();
    }

    public static NamedWriteableRegistry registryWithTestStats() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(SerializableStats.class, TestStats.NAME, TestStats::new))
        );
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
                randomTestStats()
            );
            case 1 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    randomInt(),
                    connPoolStats.getAvailableConnections(),
                    connPoolStats.getMaxConnections()
                ),
                randomTestStats()
            );
            case 2 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    randomInt(),
                    connPoolStats.getMaxConnections()
                ),
                randomTestStats()
            );
            case 3 -> new GetInferenceDiagnosticsAction.NodeResponse(
                instance.getNode(),
                new PoolStats(
                    connPoolStats.getLeasedConnections(),
                    connPoolStats.getPendingConnections(),
                    connPoolStats.getAvailableConnections(),
                    randomInt()
                ),
                randomTestStats()
            );
            default -> throw new UnsupportedEncodingException(Strings.format("Encountered unsupported case %s", select));
        };
    }

    public static SerializableStats randomTestStats() {
        return new TestStats(randomInt());
    }

    public record TestStats(int count) implements SerializableStats {
        public static final String NAME = "test_stats";

        public TestStats(StreamInput in) throws IOException {
            this(in.readInt());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("count", count).endObject();
        }
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstanceForVersion(
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
