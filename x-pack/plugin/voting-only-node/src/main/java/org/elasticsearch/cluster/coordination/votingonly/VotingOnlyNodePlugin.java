/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.cluster.coordination.votingonly;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.Join;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A voting-only node is one with the 'master' and 'voting-only' roles, dictating
 * that the node may vote in master elections but is ineligible to be master.
 */
public class VotingOnlyNodePlugin extends Plugin implements ClusterCoordinationPlugin, NetworkPlugin, ActionPlugin {

    private static final String VOTING_ONLY_ELECTION_STRATEGY = "supports_voting_only";

    private final Settings settings;
    private final SetOnce<ThreadPool> threadPool;

    private final boolean isVotingOnlyNode;

    public VotingOnlyNodePlugin(Settings settings) {
        this.settings = settings;
        threadPool = new SetOnce<>();
        isVotingOnlyNode = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
    }

    public static boolean isVotingOnlyNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
    }

    public static boolean isFullMasterNode(DiscoveryNode discoveryNode) {
        return discoveryNode.isMasterNode() && discoveryNode.getRoles().contains(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE) == false;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.threadPool.set(services.threadPool());
        return Collections.emptyList();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.VOTING_ONLY, VotingOnlyNodeFeatureSet.UsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.VOTING_ONLY, VotingOnlyNodeFeatureSet.UsageInfoAction.class)
        );
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.singletonMap(VOTING_ONLY_ELECTION_STRATEGY, new VotingOnlyNodeElectionStrategy());
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        if (isVotingOnlyNode) {
            return Collections.singletonList(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new VotingOnlyNodeAsyncSender(sender, threadPool::get);
                }
            });
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder().put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), VOTING_ONLY_ELECTION_STRATEGY).build();
    }

    static class VotingOnlyNodeElectionStrategy extends ElectionStrategy {

        @Override
        public boolean satisfiesAdditionalQuorumConstraints(
            DiscoveryNode localNode,
            long localCurrentTerm,
            long localAcceptedTerm,
            long localAcceptedVersion,
            VotingConfiguration lastCommittedConfiguration,
            VotingConfiguration lastAcceptedConfiguration,
            VoteCollection joinVotes
        ) {
            // if local node is voting only, have additional checks on election quorum definition
            if (isVotingOnlyNode(localNode)) {
                // if all votes are from voting only nodes, do not elect as master (no need to transfer state)
                if (joinVotes.nodes().stream().filter(DiscoveryNode::isMasterNode).allMatch(VotingOnlyNodePlugin::isVotingOnlyNode)) {
                    return false;
                }
                // if there's a vote from a full master node with same state (i.e. last accepted term and version match), then that node
                // should become master instead, so we should stand down. There are two exceptional cases, however:
                // 1) if we are in term 0. In that case, we allow electing the voting-only node to avoid poisonous situations where only
                // voting-only nodes are bootstrapped.
                // 2) if there is another full master node with an older state. In that case, we ensure that
                // satisfiesAdditionalQuorumConstraints cannot go from true to false when adding new joinVotes in the same election.
                // As voting-only nodes only broadcast the state to the full master nodes, eventually all of them will have caught up
                // and there should not be any remaining full master nodes with older state, effectively disabling election of
                // voting-only nodes.
                if (joinVotes.getJoins().stream().anyMatch(fullMasterWithSameState(localAcceptedTerm, localAcceptedVersion))
                    && localAcceptedTerm > 0
                    && joinVotes.getJoins().stream().noneMatch(fullMasterWithOlderState(localAcceptedTerm, localAcceptedVersion))) {
                    return false;
                }
            }
            return true;
        }

        private static Predicate<Join> fullMasterWithSameState(long localAcceptedTerm, long localAcceptedVersion) {
            return join -> isFullMasterNode(join.votingNode())
                && join.lastAcceptedTerm() == localAcceptedTerm
                && join.lastAcceptedVersion() == localAcceptedVersion;
        }

        private static Predicate<Join> fullMasterWithOlderState(long localAcceptedTerm, long localAcceptedVersion) {
            return join -> isFullMasterNode(join.votingNode())
                && (join.lastAcceptedTerm() < localAcceptedTerm
                    || (join.lastAcceptedTerm() == localAcceptedTerm && join.lastAcceptedVersion() < localAcceptedVersion));
        }
    }

    static class VotingOnlyNodeAsyncSender implements TransportInterceptor.AsyncSender {
        private final TransportInterceptor.AsyncSender sender;
        private final Supplier<ThreadPool> threadPoolSupplier;

        VotingOnlyNodeAsyncSender(TransportInterceptor.AsyncSender sender, Supplier<ThreadPool> threadPoolSupplier) {
            this.sender = sender;
            this.threadPoolSupplier = threadPoolSupplier;
        }

        @Override
        public <T extends TransportResponse> void sendRequest(
            Transport.Connection connection,
            String action,
            TransportRequest request,
            TransportRequestOptions options,
            TransportResponseHandler<T> handler
        ) {
            if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                final DiscoveryNode destinationNode = connection.getNode();
                if (isFullMasterNode(destinationNode)) {
                    sender.sendRequest(connection, action, request, options, new TransportResponseHandler<>() {
                        @Override
                        public void handleResponse(TransportResponse response) {
                            handler.handleException(
                                new TransportException(
                                    new ElasticsearchException(
                                        "ignoring successful publish response used purely for state transfer: " + response
                                    )
                                )
                            );
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            handler.handleException(exp);
                        }

                        @Override
                        public Executor executor() {
                            return handler.executor();
                        }

                        @Override
                        public TransportResponse read(StreamInput in) throws IOException {
                            return handler.read(in);
                        }
                    });
                } else {
                    threadPoolSupplier.get()
                        .generic()
                        .execute(
                            () -> handler.handleException(
                                new TransportException(
                                    new ElasticsearchException("voting-only node skipping publication to " + destinationNode)
                                )
                            )
                        );
                }
            } else {
                sender.sendRequest(connection, action, request, options, handler);
            }
        }
    }
}
