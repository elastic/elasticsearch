/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class JoinRequest extends AbstractTransportRequest {

    static final TransportVersion JOIN_REQUEST_ATTRIBUTES = TransportVersion.fromName("join_request_attributes");

    /**
     * The sending (i.e. joining) node.
     */
    private final DiscoveryNode sourceNode;

    /**
     * The compatibility versions used by the sending node.
     */
    private final CompatibilityVersions compatibilityVersions;

    /**
     * The features that are supported by the joining node.
     */
    private final Set<String> features;

    /**
     * The minimum term for which the joining node will accept any cluster state publications. If the joining node is in a strictly greater
     * term than the master it wants to join then the master must enter a new term and hold another election. Doesn't necessarily match
     * {@link JoinRequest#optionalJoin}.
     */
    private final long minimumTerm;

    /**
     * A vote for the receiving node. This vote is optional since the sending node may have voted for a different master in this term.
     * That's ok, the sender likely discovered that the master we voted for lost the election and now we're trying to join the winner. Once
     * the sender has successfully joined the master, the lack of a vote in its term causes another election (see
     * {@link Publication#onMissingJoin(DiscoveryNode)}).
     */
    private final Optional<Join> optionalJoin;

    /**
     * Facts about the sending node from plugins, computed when the request is sent, for the {@link JoinAttributesValidator}s on the master.
     */
    private final Map<String, String> joinAttributes;

    public JoinRequest(
        DiscoveryNode sourceNode,
        CompatibilityVersions compatibilityVersions,
        Set<String> features,
        long minimumTerm,
        Optional<Join> optionalJoin
    ) {
        this(sourceNode, compatibilityVersions, features, minimumTerm, optionalJoin, Map.of());
    }

    public JoinRequest(
        DiscoveryNode sourceNode,
        CompatibilityVersions compatibilityVersions,
        Set<String> features,
        long minimumTerm,
        Optional<Join> optionalJoin,
        Map<String, String> joinAttributes
    ) {
        assert optionalJoin.isPresent() == false || optionalJoin.get().votingNode().equals(sourceNode);
        this.sourceNode = sourceNode;
        this.compatibilityVersions = compatibilityVersions;
        this.features = features;
        this.minimumTerm = minimumTerm;
        this.optionalJoin = optionalJoin;
        this.joinAttributes = Objects.requireNonNull(joinAttributes);
    }

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        compatibilityVersions = CompatibilityVersions.readVersion(in);
        features = in.readCollectionAsSet(StreamInput::readString);
        minimumTerm = in.readLong();
        optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
        if (in.getTransportVersion().supports(JOIN_REQUEST_ATTRIBUTES)) {
            joinAttributes = in.readImmutableMap(StreamInput::readString);
        } else {
            joinAttributes = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        compatibilityVersions.writeTo(out);
        out.writeCollection(features, StreamOutput::writeString);
        out.writeLong(minimumTerm);
        out.writeOptionalWriteable(optionalJoin.orElse(null));
        if (out.getTransportVersion().supports(JOIN_REQUEST_ATTRIBUTES)) {
            out.writeMap(joinAttributes, StreamOutput::writeString);
        }
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public CompatibilityVersions getCompatibilityVersions() {
        return compatibilityVersions;
    }

    public Set<String> getFeatures() {
        return features;
    }

    public long getMinimumTerm() {
        return minimumTerm;
    }

    public long getTerm() {
        // If the join is also present then its term will normally equal the corresponding term, but we do not require callers to
        // obtain the term and the join in a synchronized fashion so it's possible that they disagree. Also older nodes do not share the
        // minimum term, so for BWC we can take it from the join if present.
        return Math.max(minimumTerm, optionalJoin.map(Join::term).orElse(0L));
    }

    public Optional<Join> getOptionalJoin() {
        return optionalJoin;
    }

    public Map<String, String> getJoinAttributes() {
        return joinAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof JoinRequest) == false) return false;

        JoinRequest that = (JoinRequest) o;

        if (minimumTerm != that.minimumTerm) return false;
        if (sourceNode.equals(that.sourceNode) == false) return false;
        if (compatibilityVersions.equals(that.compatibilityVersions) == false) return false;
        if (features.equals(that.features) == false) return false;
        if (optionalJoin.equals(that.optionalJoin) == false) return false;
        return joinAttributes.equals(that.joinAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, compatibilityVersions, features, minimumTerm, optionalJoin, joinAttributes);
    }

    @Override
    public String toString() {
        return "JoinRequest{"
            + "sourceNode="
            + sourceNode
            + ", compatibilityVersions="
            + compatibilityVersions
            + ", features="
            + features
            + ", minimumTerm="
            + minimumTerm
            + ", optionalJoin="
            + optionalJoin
            + ", joinAttributes="
            + joinAttributes
            + '}';
    }
}
