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

    private final Set<DiscoveryNode> votingTombstones;

    private static final ParseField TERM_PARSE_FIELD = new ParseField("term");
    private static final ParseField LAST_COMMITTED_CONFIGURATION_FIELD = new ParseField("last_committed_config");
    private static final ParseField LAST_ACCEPTED_CONFIGURATION_FIELD = new ParseField("last_accepted_config");

    private static long term(Object[] termAndConfigs) {
        return (long)termAndConfigs[0];
    }

    private static VotingConfiguration lastCommittedConfig(Object[] termAndConfig) {
        List<String> nodeIds = (List<String>) termAndConfig[1];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    private static VotingConfiguration lastAcceptedConfig(Object[] termAndConfig) {
        List<String> nodeIds = (List<String>) termAndConfig[2];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    private static final ConstructingObjectParser<CoordinationMetaData, Void> PARSER = new ConstructingObjectParser<>(
            "coordination",
            termAndConfigs -> new CoordinationMetaData(term(termAndConfigs), lastCommittedConfig(termAndConfigs),
                    lastAcceptedConfig(termAndConfigs), Collections.emptySet()));
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TERM_PARSE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_COMMITTED_CONFIGURATION_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_ACCEPTED_CONFIGURATION_FIELD);
    }

    public CoordinationMetaData(long term, VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
        Set<DiscoveryNode> votingTombstones) {
        this.term = term;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.votingTombstones = votingTombstones;
    }

    public CoordinationMetaData(StreamInput in) throws IOException {
        term = in.readLong();
        lastCommittedConfiguration = new VotingConfiguration(in);
        lastAcceptedConfiguration = new VotingConfiguration(in);
        votingTombstones = Collections.unmodifiableSet(in.readSet(DiscoveryNode::new));
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
            .field(LAST_ACCEPTED_CONFIGURATION_FIELD.getPreferredName(), lastAcceptedConfiguration);
        // TODO include voting tombstones here
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

    public Set<DiscoveryNode> getVotingTombstones() {
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
        private final Set<DiscoveryNode> votingTombstones = new HashSet<>();

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

        public Builder addVotingTombstone(DiscoveryNode tombstone) {
            votingTombstones.add(tombstone);
            return this;
        }

        public Builder clearVotingTombstones() {
            votingTombstones.clear();
            return this;
        }

        public CoordinationMetaData build() {
            return new CoordinationMetaData(term, lastCommittedConfiguration, lastAcceptedConfiguration,
                Collections.unmodifiableSet(new HashSet<>(votingTombstones)));
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
