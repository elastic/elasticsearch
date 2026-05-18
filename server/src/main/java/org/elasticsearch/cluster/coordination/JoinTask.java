/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.collect.Iterators;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public record JoinTask(List<NodeJoinTask> nodeJoinTasks, boolean isBecomingMaster, long term, ClusterState initialState)
    implements
        ClusterStateTaskListener {

    public static JoinTask singleNode(
        DiscoveryNode node,
        CompatibilityVersions compatibilityVersions,
        Set<String> features,
        JoinReason reason,
        ActionListener<Void> listener,
        long term
    ) {
        return new JoinTask(List.of(new NodeJoinTask(node, compatibilityVersions, features, reason, listener)), false, term, null);
    }

    public static JoinTask completingElection(Stream<NodeJoinTask> nodeJoinTaskStream, long term) {
        return new JoinTask(nodeJoinTaskStream.toList(), true, term, null);
    }

    public int nodeCount() {
        return nodeJoinTasks.size();
    }

    @Override
    public void onFailure(Exception e) {
        for (NodeJoinTask nodeJoinTask : nodeJoinTasks) {
            nodeJoinTask.listener.onFailure(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();

        if (isBecomingMaster) {
            stringBuilder.append("_FINISH_ELECTION_");
        }

        for (NodeJoinTask nodeJoinTask : nodeJoinTasks) {
            if (stringBuilder.isEmpty() == false) {
                stringBuilder.append(", ");
            }
            nodeJoinTask.appendDescription(stringBuilder);
        }

        return stringBuilder.toString();
    }

    public Iterable<DiscoveryNode> nodes() {
        return () -> Iterators.map(nodeJoinTasks.iterator(), j -> j.node);
    }

    public JoinTask alsoRefreshState(ClusterState latestState) {
        assert isBecomingMaster;
        return new JoinTask(nodeJoinTasks, isBecomingMaster, term, latestState);
    }

    public record NodeJoinTask(
        DiscoveryNode node,
        CompatibilityVersions compatibilityVersions,
        Set<String> features,
        JoinReason reason,
        ActionListener<Void> listener
    ) {

        public NodeJoinTask(
            DiscoveryNode node,
            CompatibilityVersions compatibilityVersions,
            Set<String> features,
            JoinReason reason,
            ActionListener<Void> listener
        ) {
            this.node = Objects.requireNonNull(node);
            this.compatibilityVersions = Objects.requireNonNull(compatibilityVersions);
            this.features = Objects.requireNonNull(features);
            this.reason = reason;
            this.listener = listener;
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            appendDescription(stringBuilder);
            return stringBuilder.toString();
        }

        public void appendDescription(StringBuilder stringBuilder) {
            node.appendDescriptionWithoutAttributes(stringBuilder);
            stringBuilder.append(' ').append(reason.message());
        }
    }
}
