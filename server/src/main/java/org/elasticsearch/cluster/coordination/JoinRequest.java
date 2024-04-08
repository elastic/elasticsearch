/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class JoinRequest extends TransportRequest {

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

    public JoinRequest(
        DiscoveryNode sourceNode,
        CompatibilityVersions compatibilityVersions,
        Set<String> features,
        long minimumTerm,
        Optional<Join> optionalJoin
    ) {
        assert optionalJoin.isPresent() == false || optionalJoin.get().votingNode().equals(sourceNode);
        this.sourceNode = sourceNode;
        this.compatibilityVersions = compatibilityVersions;
        this.features = features;
        this.minimumTerm = minimumTerm;
        this.optionalJoin = optionalJoin;
    }

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            compatibilityVersions = CompatibilityVersions.readVersion(in);
        } else {
            // there's a 1-1 mapping from Version to TransportVersion before 8.8.0
            // no known mapping versions here
            compatibilityVersions = new CompatibilityVersions(
                TransportVersion.fromId(sourceNode.getPre811VersionId().getAsInt()),
                Map.of()
            );
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            features = in.readCollectionAsSet(StreamInput::readString);
        } else {
            features = Set.of();
        }
        minimumTerm = in.readLong();
        optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            compatibilityVersions.writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeCollection(features, StreamOutput::writeString);
        }
        out.writeLong(minimumTerm);
        out.writeOptionalWriteable(optionalJoin.orElse(null));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof JoinRequest) == false) return false;

        JoinRequest that = (JoinRequest) o;

        if (minimumTerm != that.minimumTerm) return false;
        if (sourceNode.equals(that.sourceNode) == false) return false;
        if (compatibilityVersions.equals(that.compatibilityVersions) == false) return false;
        if (features.equals(that.features) == false) return false;
        return optionalJoin.equals(that.optionalJoin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, compatibilityVersions, features, minimumTerm, optionalJoin);
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
            + '}';
    }
}
