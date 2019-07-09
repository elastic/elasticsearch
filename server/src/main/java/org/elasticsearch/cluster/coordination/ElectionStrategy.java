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

import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Allows plugging in a custom election strategy, restricting the notion of an election quorum.
 * Custom additional quorum restrictions can be defined by implementing the {@link #satisfiesAdditionalQuorumConstraints} method.
 */
public abstract class ElectionStrategy {

    public static final ElectionStrategy DEFAULT_INSTANCE = new ElectionStrategy() {
        @Override
        protected boolean satisfiesAdditionalQuorumConstraints(DiscoveryNode localNode, long localCurrentTerm, long localAcceptedTerm,
                                                               long localAcceptedVersion, VotingConfiguration lastCommittedConfiguration,
                                                               VotingConfiguration lastAcceptedConfiguration, VoteCollection joinVotes) {
            return true;
        }
    };

    protected ElectionStrategy() {

    }

    /**
     * Whether there is an election quorum from the point of view of the given local node under the provided voting configurations
     */
    public final boolean isElectionQuorum(DiscoveryNode localNode, long localCurrentTerm, long localAcceptedTerm, long localAcceptedVersion,
                                          VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                                          VoteCollection joinVotes) {
        return joinVotes.isQuorum(lastCommittedConfiguration) &&
            joinVotes.isQuorum(lastAcceptedConfiguration) &&
            satisfiesAdditionalQuorumConstraints(localNode, localCurrentTerm, localAcceptedTerm, localAcceptedVersion,
                lastCommittedConfiguration, lastAcceptedConfiguration, joinVotes);
    }

    /**
     * The extension point to be overridden by plugins. Defines additional constraints on the election quorum.
     * @param localNode                  the local node for the election quorum
     * @param localCurrentTerm           the current term of the local node
     * @param localAcceptedTerm          the last accepted term of the local node
     * @param localAcceptedVersion       the last accepted version of the local node
     * @param lastCommittedConfiguration the last committed configuration for the election quorum
     * @param lastAcceptedConfiguration  the last accepted configuration for the election quorum
     * @param joinVotes                  the votes that were provided so far
     * @return true iff the additional quorum constraints are satisfied
     */
    protected abstract boolean satisfiesAdditionalQuorumConstraints(DiscoveryNode localNode,
                                                                    long localCurrentTerm,
                                                                    long localAcceptedTerm,
                                                                    long localAcceptedVersion,
                                                                    VotingConfiguration lastCommittedConfiguration,
                                                                    VotingConfiguration lastAcceptedConfiguration,
                                                                    VoteCollection joinVotes);
}
