/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CoordinationMetadata implements Writeable, ToXContentFragment {

    public static final CoordinationMetadata EMPTY_METADATA = builder().build();

    private final long term;

    private final VotingConfiguration lastCommittedConfiguration;

    private final VotingConfiguration lastAcceptedConfiguration;

    private final Set<VotingConfigExclusion> votingConfigExclusions;

    private static final ParseField TERM_PARSE_FIELD = new ParseField("term");
    private static final ParseField LAST_COMMITTED_CONFIGURATION_FIELD = new ParseField("last_committed_config");
    private static final ParseField LAST_ACCEPTED_CONFIGURATION_FIELD = new ParseField("last_accepted_config");
    private static final ParseField VOTING_CONFIG_EXCLUSIONS_FIELD = new ParseField("voting_config_exclusions");

    private static long term(Object[] termAndConfigs) {
        return (long)termAndConfigs[0];
    }

    @SuppressWarnings("unchecked")
    private static VotingConfiguration lastCommittedConfig(Object[] fields) {
        List<String> nodeIds = (List<String>) fields[1];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    @SuppressWarnings("unchecked")
    private static VotingConfiguration lastAcceptedConfig(Object[] fields) {
        List<String> nodeIds = (List<String>) fields[2];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    @SuppressWarnings("unchecked")
    private static Set<VotingConfigExclusion> votingConfigExclusions(Object[] fields) {
        Set<VotingConfigExclusion> votingTombstones = new HashSet<>((List<VotingConfigExclusion>) fields[3]);
        return votingTombstones;
    }

    private static final ConstructingObjectParser<CoordinationMetadata, Void> PARSER = new ConstructingObjectParser<>(
            "coordination_metadata",
            fields -> new CoordinationMetadata(term(fields), lastCommittedConfig(fields),
                    lastAcceptedConfig(fields), votingConfigExclusions(fields)));
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TERM_PARSE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_COMMITTED_CONFIGURATION_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_ACCEPTED_CONFIGURATION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), VotingConfigExclusion.PARSER, VOTING_CONFIG_EXCLUSIONS_FIELD);
    }

    public CoordinationMetadata(long term, VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                                Set<VotingConfigExclusion> votingConfigExclusions) {
        this.term = term;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.votingConfigExclusions = Set.copyOf(votingConfigExclusions);
    }

    public CoordinationMetadata(StreamInput in) throws IOException {
        term = in.readLong();
        lastCommittedConfiguration = new VotingConfiguration(in);
        lastAcceptedConfiguration = new VotingConfiguration(in);
        votingConfigExclusions = Collections.unmodifiableSet(in.readSet(VotingConfigExclusion::new));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(CoordinationMetadata coordinationMetadata) {
        return new Builder(coordinationMetadata);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(term);
        lastCommittedConfiguration.writeTo(out);
        lastAcceptedConfiguration.writeTo(out);
        out.writeCollection(votingConfigExclusions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .field(TERM_PARSE_FIELD.getPreferredName(), term)
            .field(LAST_COMMITTED_CONFIGURATION_FIELD.getPreferredName(), lastCommittedConfiguration)
            .field(LAST_ACCEPTED_CONFIGURATION_FIELD.getPreferredName(), lastAcceptedConfiguration)
            .field(VOTING_CONFIG_EXCLUSIONS_FIELD.getPreferredName(), votingConfigExclusions);
    }

    public static CoordinationMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public long term() {
        return term;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return lastAcceptedConfiguration;
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return lastCommittedConfiguration;
    }

    public Set<VotingConfigExclusion> getVotingConfigExclusions() {
        return votingConfigExclusions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof CoordinationMetadata) == false) return false;

        CoordinationMetadata that = (CoordinationMetadata) o;

        if (term != that.term) return false;
        if (lastCommittedConfiguration.equals(that.lastCommittedConfiguration) == false) return false;
        if (lastAcceptedConfiguration.equals(that.lastAcceptedConfiguration) == false) return false;
        return votingConfigExclusions.equals(that.votingConfigExclusions);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + lastCommittedConfiguration.hashCode();
        result = 31 * result + lastAcceptedConfiguration.hashCode();
        result = 31 * result + votingConfigExclusions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CoordinationMetadata{" +
            "term=" + term +
            ", lastCommittedConfiguration=" + lastCommittedConfiguration +
            ", lastAcceptedConfiguration=" + lastAcceptedConfiguration +
            ", votingConfigExclusions=" + votingConfigExclusions +
            '}';
    }

    public static class Builder {
        private long term = 0;
        private VotingConfiguration lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private VotingConfiguration lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private final Set<VotingConfigExclusion> votingConfigExclusions = new HashSet<>();

        public Builder() {

        }

        public Builder(CoordinationMetadata state) {
            this.term = state.term;
            this.lastCommittedConfiguration = state.lastCommittedConfiguration;
            this.lastAcceptedConfiguration = state.lastAcceptedConfiguration;
            this.votingConfigExclusions.addAll(state.votingConfigExclusions);
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder lastCommittedConfiguration(VotingConfiguration config) {
            this.lastCommittedConfiguration = config;
            return this;
        }

        public Builder lastAcceptedConfiguration(VotingConfiguration config) {
            this.lastAcceptedConfiguration = config;
            return this;
        }

        public Builder addVotingConfigExclusion(VotingConfigExclusion exclusion) {
            votingConfigExclusions.add(exclusion);
            return this;
        }

        public Builder clearVotingConfigExclusions() {
            votingConfigExclusions.clear();
            return this;
        }

        public CoordinationMetadata build() {
            return new CoordinationMetadata(term, lastCommittedConfiguration, lastAcceptedConfiguration, votingConfigExclusions);
        }
    }

    public static class VotingConfigExclusion implements Writeable, ToXContentFragment {
        public static final String MISSING_VALUE_MARKER = "_absent_";
        private final String nodeId;
        private final String nodeName;

        public VotingConfigExclusion(DiscoveryNode node) {
            this(node.getId(), node.getName());
        }

        public VotingConfigExclusion(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.nodeName = in.readString();
        }

        public VotingConfigExclusion(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(nodeName);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getNodeName() {
            return nodeName;
        }

        private static final ParseField NODE_ID_PARSE_FIELD = new ParseField("node_id");
        private static final ParseField NODE_NAME_PARSE_FIELD = new ParseField("node_name");

        private static String nodeId(Object[] nodeIdAndName) {
            return (String) nodeIdAndName[0];
        }

        private static String nodeName(Object[] nodeIdAndName) {
            return (String) nodeIdAndName[1];
        }

        private static final ConstructingObjectParser<VotingConfigExclusion, Void> PARSER = new ConstructingObjectParser<>(
                "voting_config_exclusion",
                nodeIdAndName -> new VotingConfigExclusion(nodeId(nodeIdAndName), nodeName(nodeIdAndName))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_PARSE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_NAME_PARSE_FIELD);
        }

        public static VotingConfigExclusion fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(NODE_ID_PARSE_FIELD.getPreferredName(), nodeId)
                    .field(NODE_NAME_PARSE_FIELD.getPreferredName(), nodeName)
                    .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VotingConfigExclusion that = (VotingConfigExclusion) o;
            return Objects.equals(nodeId, that.nodeId) &&
                    Objects.equals(nodeName, that.nodeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, nodeName);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (nodeName.length() > 0) {
                sb.append('{').append(nodeName).append('}');
            }
            sb.append('{').append(nodeId).append('}');
            return sb.toString();
        }

    }

    /**
     * A collection of persistent node ids, denoting the voting configuration for cluster state changes.
     */
    public static class VotingConfiguration implements Writeable, ToXContentFragment {

        public static final VotingConfiguration EMPTY_CONFIG = new VotingConfiguration(Collections.emptySet());
        public static final VotingConfiguration MUST_JOIN_ELECTED_MASTER = new VotingConfiguration(Collections.singleton(
                "_must_join_elected_master_"));

        private final Set<String> nodeIds;

        public VotingConfiguration(Set<String> nodeIds) {
            this.nodeIds = Set.copyOf(nodeIds);
        }

        public VotingConfiguration(StreamInput in) throws IOException {
            nodeIds = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(nodeIds.toArray(new String[0]));
        }

        public boolean hasQuorum(Collection<String> votes) {
            final HashSet<String> intersection = new HashSet<>(nodeIds);
            intersection.retainAll(votes);
            return intersection.size() * 2 > nodeIds.size();
        }

        public Set<String> getNodeIds() {
            return nodeIds;
        }

        @Override
        public String toString() {
            return "VotingConfiguration{" + String.join(",", nodeIds) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VotingConfiguration that = (VotingConfiguration) o;
            return Objects.equals(nodeIds, that.nodeIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeIds);
        }

        public boolean isEmpty() {
            return nodeIds.isEmpty();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (String nodeId : nodeIds) {
                builder.value(nodeId);
            }
            return builder.endArray();
        }

        public static VotingConfiguration of(DiscoveryNode... nodes) {
            return new VotingConfiguration(Arrays.stream(nodes).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        }
    }
}
