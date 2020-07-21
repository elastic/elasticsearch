/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.reelect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.PreVoteRequest;
import org.elasticsearch.cluster.coordination.PreVoteResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.coordination.PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME;

public class TransportClusterReelectAction extends TransportMasterNodeAction<ClusterReelectRequest, ClusterReelectResponse> {

    private final Logger logger = LogManager.getLogger(getClass());
    private final Discovery discovery;

    @Inject
    public TransportClusterReelectAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Discovery discovery) throws UnsupportedOperationException {
        super(ClusterReelectAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClusterReelectRequest::new, indexNameExpressionResolver);

        this.discovery = discovery;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterReelectResponse read(StreamInput in) throws IOException {
        return new ClusterReelectResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterReelectRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, final ClusterReelectRequest request, final ClusterState state,
                                   final ActionListener<ClusterReelectResponse> listener) {
        assert discovery instanceof Coordinator;
        final Coordinator coordinator = ((Coordinator) discovery);
        final String[] preferredMasters = coordinator.getPreferredMasters();
        final ClusterState lastAcceptedState = coordinator.getLastAcceptedState();
        if (String.join(",", preferredMasters).equals("")) {
            // preferred master has not been set, reelect randomly
            final List<DiscoveryNode> masterCandidates = coordinator.getMasterCandidates(lastAcceptedState,
                Arrays.asList(lastAcceptedState.getNodes().getMasterNodes().values().toArray(DiscoveryNode.class)));
            coordinator.atomicAbdicateTo(masterCandidates.get(new Random(Randomness.get().nextLong()).nextInt(masterCandidates.size())));
            listener.onResponse(new ClusterReelectResponse(true));
            return;
        }

        ArrayList<DiscoveryNode> preferredNodes = new ArrayList<>();
        for (String preferredMaster : preferredMasters) {
            if (preferredMaster.equals(lastAcceptedState.nodes().getMasterNode().getName())) {
                listener.onResponse(new ClusterReelectResponse(true, "preferred master node has already been elected"));
                return;
            }

            Iterator<DiscoveryNode> iterator = lastAcceptedState.getNodes().getMasterNodes().valuesIt();
            while (iterator.hasNext()) {
                DiscoveryNode node = iterator.next();
                if (node.getName().equals(preferredMaster) && Coordinator.nodeMayWinElection(lastAcceptedState, node)) {
                    preferredNodes.add(node);
                }
            }
        }

        if (preferredNodes.size() == 0) {
            listener.onResponse(new ClusterReelectResponse(false, "none of preferred master node can be elected"));
            return;
        }

        // check prefer master's cluster state
        AtomicInteger lagPreferredNodesCount = new AtomicInteger(0);
        for (DiscoveryNode node : preferredNodes) {
            // leader send preVote request to preferred master nodes and compare last accepted state version to avoid useless waiting
            PreVoteRequest preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), lastAcceptedState.term());
            transportService.sendRequest(node, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public PreVoteResponse read(StreamInput in) throws IOException {
                        return new PreVoteResponse(in);
                    }

                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        if (response.getLastAcceptedVersion() < lastAcceptedState.version()) {
                            logger.info("ignoring {} from {} as cluster version is older than leader", response, node);
                            if (lagPreferredNodesCount.incrementAndGet() == preferredNodes.size()) {
                                listener.onResponse(new ClusterReelectResponse(false,
                                    "none of prefer master's state is catching up with leader"));
                            }
                            return;
                        }

                        logger.info("the preferred master {} has the newest state", node);
                        coordinator.atomicAbdicateTo(node);
                        listener.onResponse(new ClusterReelectResponse(true));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onResponse(new ClusterReelectResponse(false, "check cluster state failed: " + exp));
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                });
        }
    }

}
