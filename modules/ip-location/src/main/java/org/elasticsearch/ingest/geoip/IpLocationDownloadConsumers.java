/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Per-project cluster state metadata tracking which consumer types have requested
 * IP database downloads. Used as the single, cluster-wide source of truth so that:
 * <ul>
 *   <li>The {@link GeoIpDownloader} (Step 1: download to index) knows whether any
 *       consumer has requested databases for a project.</li>
 *   <li>{@link DatabaseNodeService#checkDatabases} (Step 2: copy to local disk) can
 *       filter by consumer-type vs. local node role.</li>
 * </ul>
 */
public final class IpLocationDownloadConsumers implements Metadata.ProjectCustom {

    private static final Logger logger = LogManager.getLogger(IpLocationDownloadConsumers.class);

    public static final String TYPE = "ip_location_download_consumers";
    /**
     * Transport version gating wire serialization of this project custom. Older nodes that do not
     * declare this version are excluded by {@code VersionedNamedWriteable.writeVersionedWriteables},
     * so they never receive an unknown custom name during a rolling upgrade.
     */
    public static final TransportVersion IP_LOCATION_DOWNLOAD_CONSUMERS = TransportVersion.fromName("ip_location_download_consumers");
    private static final ParseField CONSUMERS_FIELD = new ParseField("consumers");

    public static final IpLocationDownloadConsumers EMPTY = new IpLocationDownloadConsumers(EnumSet.noneOf(IpLocationConsumer.class));

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IpLocationDownloadConsumers, Void> PARSER = new ConstructingObjectParser<>(TYPE, a -> {
        List<String> names = (List<String>) a[0];
        EnumSet<IpLocationConsumer> consumers = EnumSet.noneOf(IpLocationConsumer.class);
        for (String name : names) {
            try {
                consumers.add(IpLocationConsumer.valueOf(name));
            } catch (IllegalArgumentException e) {
                // Tolerate unknown values so a snapshot or cluster-state restored from a newer cluster (which may
                // declare additional consumers we don't know about) does not fail to parse on this node. The wire
                // path is independently protected by the TransportVersion gating in getMinimalSupportedVersion().
                logger.warn("ignoring unknown ip-location download consumer [{}] while parsing project custom [{}]", name, TYPE);
            }
        }
        return new IpLocationDownloadConsumers(consumers);
    });

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), CONSUMERS_FIELD);
    }

    private final EnumSet<IpLocationConsumer> consumers;

    public IpLocationDownloadConsumers(Set<IpLocationConsumer> consumers) {
        this.consumers = consumers.isEmpty() ? EnumSet.noneOf(IpLocationConsumer.class) : EnumSet.copyOf(consumers);
    }

    public IpLocationDownloadConsumers(StreamInput in) throws IOException {
        this.consumers = in.readEnumSet(IpLocationConsumer.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnumSet(consumers);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return IP_LOCATION_DOWNLOAD_CONSUMERS;
    }

    public boolean hasConsumers() {
        return consumers.isEmpty() == false;
    }

    public boolean contains(IpLocationConsumer consumer) {
        return consumers.contains(consumer);
    }

    public IpLocationDownloadConsumers withConsumer(IpLocationConsumer consumer) {
        if (consumers.contains(consumer)) {
            return this;
        }
        EnumSet<IpLocationConsumer> updated = EnumSet.copyOf(consumers);
        updated.add(consumer);
        return new IpLocationDownloadConsumers(updated);
    }

    public IpLocationDownloadConsumers withoutConsumer(IpLocationConsumer consumer) {
        if (consumers.contains(consumer) == false) {
            return this;
        }
        EnumSet<IpLocationConsumer> updated = EnumSet.copyOf(consumers);
        updated.remove(consumer);
        return new IpLocationDownloadConsumers(updated);
    }

    /**
     * Returns true if any active consumer in this metadata is relevant for the given node's roles.
     */
    public boolean hasRelevantConsumer(DiscoveryNode localNode) {
        for (IpLocationConsumer consumer : consumers) {
            if (isRelevantForNode(consumer, localNode)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRelevantForNode(IpLocationConsumer consumer, DiscoveryNode localNode) {
        return switch (consumer) {
            case INGEST -> localNode.isIngestNode();
            // Any node can coordinate an ES|QL query, and IpLocationExec can run on both the
            // coordinator plan and data-node plan, so all nodes need databases staged.
            case ESQL -> true;
        };
    }

    public static IpLocationDownloadConsumers fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.chunk((builder, params) -> {
            builder.startArray(CONSUMERS_FIELD.getPreferredName());
            for (IpLocationConsumer consumer : consumers) {
                builder.value(consumer.name());
            }
            builder.endArray();
            return builder;
        });
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // API -> diagnostics via GET _cluster/state.
        // GATEWAY -> survives a full-cluster restart so we don't re-enter the bootstrap / ESQL-first-query
        // windows on every restart.
        // NOT SNAPSHOT: consumer registrations are operational intent tied to the *running* cluster, not data
        // that should travel with a snapshot. On snapshot restore, INGEST is re-derived from the
        // restored pipelines by IngestIpLocationPlugin, and ESQL re-registers lazily on the first
        // IP_LOCATION query. Persisting the custom into snapshots would eagerly stage databases on
        // a destination cluster that may never run those pipelines or queries.
        return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
    }

    @Override
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom before) {
        return new IpLocationDownloadConsumersDiff((IpLocationDownloadConsumers) before, this);
    }

    /**
     * Snapshot-style diff (mirrors {@link org.elasticsearch.cluster.AbstractNamedDiffable}): when {@code before} and
     * {@code after} are equal we serialize a single {@code false} byte and {@link #apply(Metadata.ProjectCustom)} is
     * an identity; otherwise we serialize the full {@code after} state. There is no benefit in computing a true
     * added/removed delta over a tiny {@link EnumSet} of {@link IpLocationConsumer} values.
     */
    static class IpLocationDownloadConsumersDiff implements NamedDiff<Metadata.ProjectCustom> {

        @Nullable
        private final EnumSet<IpLocationConsumer> consumers;

        IpLocationDownloadConsumersDiff(IpLocationDownloadConsumers before, IpLocationDownloadConsumers after) {
            if (before != null && before.consumers.equals(after.consumers)) {
                this.consumers = null;
            } else {
                this.consumers = after.consumers.isEmpty() ? EnumSet.noneOf(IpLocationConsumer.class) : EnumSet.copyOf(after.consumers);
            }
        }

        IpLocationDownloadConsumersDiff(StreamInput in) throws IOException {
            this.consumers = in.readBoolean() ? in.readEnumSet(IpLocationConsumer.class) : null;
        }

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            return consumers == null ? part : new IpLocationDownloadConsumers(consumers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (consumers == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeEnumSet(consumers);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return IP_LOCATION_DOWNLOAD_CONSUMERS;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IpLocationDownloadConsumers that = (IpLocationDownloadConsumers) o;
        return Objects.equals(consumers, that.consumers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumers);
    }

    @Override
    public String toString() {
        return "IpLocationDownloadConsumers{consumers=" + consumers + "}";
    }
}
