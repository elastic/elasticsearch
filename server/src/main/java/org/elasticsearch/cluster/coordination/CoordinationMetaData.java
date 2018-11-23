/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
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

public class CoordinationMetaData implements Writeable, ToXContentFragment {

    public static final CoordinationMetaData EMPTY_META_DATA = builder().build();

    private final long term;

    private final VotingConfiguration lastCommittedConfiguration;

    private final VotingConfiguration lastAcceptedConfiguration;

    private final Set<VotingTombstone> votingTombstones;

    private static final ParseField TERM_PARSE_FIELD = new ParseField("term");
    private static final ParseField LAST_COMMITTED_CONFIGURATION_FIELD = new ParseField("last_committed_config");
    private static final ParseField LAST_ACCEPTED_CONFIGURATION_FIELD = new ParseField("last_accepted_config");
    private static final ParseField VOTING_TOMBSTONES_FIELD = new ParseField("voting_tombstones");

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
    private static Set<VotingTombstone> votingTombstones(Object[] fields) {
        Set<VotingTombstone> votingTombstones = new HashSet<>((List<VotingTombstone>) fields[3]);
        return votingTombstones;
    }

    private static final ConstructingObjectParser<CoordinationMetaData, Void> PARSER = new ConstructingObjectParser<>(
            "coordination_metadata",
            fields -> new CoordinationMetaData(term(fields), lastCommittedConfig(fields),
                    lastAcceptedConfig(fields), votingTombstones(fields)));
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TERM_PARSE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_COMMITTED_CONFIGURATION_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_ACCEPTED_CONFIGURATION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), VotingTombstone.PARSER, VOTING_TOMBSTONES_FIELD);
    }

    public CoordinationMetaData(long term, VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                                Set<VotingTombstone> votingTombstones) {
        this.term = term;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.votingTombstones = Collections.unmodifiableSet(new HashSet<>(votingTombstones));
    }

    public CoordinationMetaData(StreamInput in) throws IOException {
        term = in.readLong();
        lastCommittedConfiguration = new VotingConfiguration(in);
        lastAcceptedConfiguration = new VotingConfiguration(in);
        votingTombstones = Collections.unmodifiableSet(in.readSet(VotingTombstone::new));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(CoordinationMetaData coordinationMetaData) {
        return new Builder(coordinationMetaData);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(term);
        lastCommittedConfiguration.writeTo(out);
        lastAcceptedConfiguration.writeTo(out);
        out.writeCollection(votingTombstones, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .field(TERM_PARSE_FIELD.getPreferredName(), term)
            .field(LAST_COMMITTED_CONFIGURATION_FIELD.getPreferredName(), lastCommittedConfiguration)
            .field(LAST_ACCEPTED_CONFIGURATION_FIELD.getPreferredName(), lastAcceptedConfiguration)
            .field(VOTING_TOMBSTONES_FIELD.getPreferredName(), votingTombstones);
    }

    public static CoordinationMetaData fromXContent(XContentParser parser) throws IOException {
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

    public Set<VotingTombstone> getVotingTombstones() {
        return votingTombstones;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CoordinationMetaData)) return false;

        CoordinationMetaData that = (CoordinationMetaData) o;

        if (term != that.term) return false;
        if (!lastCommittedConfiguration.equals(that.lastCommittedConfiguration)) return false;
        if (!lastAcceptedConfiguration.equals(that.lastAcceptedConfiguration)) return false;
        return votingTombstones.equals(that.votingTombstones);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + lastCommittedConfiguration.hashCode();
        result = 31 * result + lastAcceptedConfiguration.hashCode();
        result = 31 * result + votingTombstones.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CoordinationMetaData{" +
            "term=" + term +
            ", lastCommittedConfiguration=" + lastCommittedConfiguration +
            ", lastAcceptedConfiguration=" + lastAcceptedConfiguration +
            ", votingTombstones=" + votingTombstones +
            '}';
    }

    public static class Builder {
        private long term = 0;
        private VotingConfiguration lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private VotingConfiguration lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private final Set<VotingTombstone> votingTombstones = new HashSet<>();

        public Builder() {

        }
        
        public Builder(CoordinationMetaData state) {
            this.term = state.term;
            this.lastCommittedConfiguration = state.lastCommittedConfiguration;
            this.lastAcceptedConfiguration = state.lastAcceptedConfiguration;
            this.votingTombstones.addAll(state.votingTombstones);
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

        public Builder addVotingTombstone(VotingTombstone tombstone) {
            votingTombstones.add(tombstone);
            return this;
        }

        public Builder clearVotingTombstones() {
            votingTombstones.clear();
            return this;
        }

        public CoordinationMetaData build() {
            return new CoordinationMetaData(term, lastCommittedConfiguration, lastAcceptedConfiguration, votingTombstones);
        }
    }

    public static class VotingTombstone implements Writeable, ToXContentFragment {
        private String nodeId;
        private String nodeName;

        public VotingTombstone(DiscoveryNode node) {
            this(node.getId(), node.getName());
        }

        public VotingTombstone(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.nodeName = in.readString();
        }

        public VotingTombstone(String nodeId, String nodeName) {
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

        private static final ConstructingObjectParser<VotingTombstone, Void> PARSER = new ConstructingObjectParser<>(
                "voting_tombstone",
                nodeIdAndName -> new VotingTombstone(nodeId(nodeIdAndName), nodeName(nodeIdAndName))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_PARSE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_NAME_PARSE_FIELD);
        }

        public static VotingTombstone fromXContent(XContentParser parser) throws IOException {
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
            VotingTombstone that = (VotingTombstone) o;
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
            sb.append('{').append(nodeId).append('}');
            if (nodeName.length() > 0) {
                sb.append('{').append(nodeName).append('}');
            }
            return sb.toString();
        }

    }

    /**
     * A collection of persistent node ids, denoting the voting configuration for cluster state changes.
     */
    public static class VotingConfiguration implements Writeable, ToXContentFragment {

        public static final VotingConfiguration EMPTY_CONFIG = new VotingConfiguration(Collections.emptySet());

        private final Set<String> nodeIds;

        public VotingConfiguration(Set<String> nodeIds) {
            this.nodeIds = Collections.unmodifiableSet(new HashSet<>(nodeIds));
        }

        public VotingConfiguration(StreamInput in) throws IOException {
            nodeIds = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(nodeIds.toArray(new String[nodeIds.size()]));
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
            // this could be used in many more places - TODO use this where appropriate
            return new VotingConfiguration(Arrays.stream(nodes).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        }
    }
}
