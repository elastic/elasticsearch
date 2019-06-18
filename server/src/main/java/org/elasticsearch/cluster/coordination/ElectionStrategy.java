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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Collection;

public interface ElectionStrategy {
    /**
     * Whether this node should abdicate to the given node when standing down as master
     */
    boolean isAbdicationTarget(DiscoveryNode discoveryNode);

    /**
     * Returns an extra filter on whether a collection of votes is a good quorum for an election
     */
    boolean isGoodQuorum(Collection<DiscoveryNode> votingNodes);

    /**
     * Whether to accept the given pre-vote
     */
    boolean acceptPrevote(PreVoteResponse response, DiscoveryNode sender, ClusterState clusterState);

    /**
     * Whether the destination node should receive publications of new cluster states
     */
    boolean shouldReceivePublication(DiscoveryNode destination);

    /**
     * Allows the strategy to modify the {@link PublishWithJoinResponse} received before it is handled by the {@link Coordinator}.
     */
    ActionListener<PublishWithJoinResponse> wrapPublishResponseHandler(ActionListener<PublishWithJoinResponse> listener);

    class DefaultElectionStrategy implements ElectionStrategy {

        public static final ElectionStrategy INSTANCE = new DefaultElectionStrategy();

        private DefaultElectionStrategy() {
        }

        @Override
        public boolean isAbdicationTarget(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public boolean isGoodQuorum(Collection<DiscoveryNode> votingNodes) {
            return true;
        }

        @Override
        public boolean acceptPrevote(PreVoteResponse response, DiscoveryNode sender, ClusterState clusterState) {
            return true;
        }

        @Override
        public boolean shouldReceivePublication(DiscoveryNode destination) {
            return true;
        }

        @Override
        public ActionListener<PublishWithJoinResponse> wrapPublishResponseHandler(ActionListener<PublishWithJoinResponse> listener) {
            return listener;
        }
    }
}
