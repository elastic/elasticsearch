/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

public class SimulatePipelineTransportAction extends HandledTransportAction<SimulatePipelineRequest, SimulatePipelineResponse> {
    private static final Logger logger = LogManager.getLogger(SimulatePipelineTransportAction.class);
    /**
     * This is the amount of time given as the timeout for transport requests to the ingest node.
     */
    public static final Setting<TimeValue> INGEST_NODE_TRANSPORT_ACTION_TIMEOUT = Setting.timeSetting(
        "ingest_node.transport_action_timeout",
        TimeValue.timeValueSeconds(20),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final IngestService ingestService;
    private final SimulateExecutionService executionService;
    private final TransportService transportService;
    private volatile TimeValue ingestNodeTransportActionTimeout;
    // ThreadLocal because our unit testing framework does not like sharing Randoms across threads
    private final ThreadLocal<Random> random = ThreadLocal.withInitial(Randomness::get);

    @Inject
    public SimulatePipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IngestService ingestService
    ) {
        super(
            SimulatePipelineAction.NAME,
            transportService,
            actionFilters,
            SimulatePipelineRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ingestService = ingestService;
        this.executionService = new SimulateExecutionService(threadPool);
        this.transportService = transportService;
        this.ingestNodeTransportActionTimeout = INGEST_NODE_TRANSPORT_ACTION_TIMEOUT.get(ingestService.getClusterService().getSettings());
        ingestService.getClusterService()
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                INGEST_NODE_TRANSPORT_ACTION_TIMEOUT,
                newTimeout -> this.ingestNodeTransportActionTimeout = newTimeout
            );
    }

    @Override
    protected void doExecute(Task task, SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        final Map<String, Object> source = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        DiscoveryNodes discoveryNodes = ingestService.getClusterService().state().nodes();
        Map<String, DiscoveryNode> ingestNodes = discoveryNodes.getIngestNodes();
        if (ingestNodes.isEmpty()) {
            /*
             * Some resources used by pipelines, such as the geoip database, only exist on ingest nodes. Since we only run pipelines on
             * nodes with the ingest role, we ought to only simulate a pipeline on nodes with the ingest role.
             */
            listener.onFailure(
                new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.")
            );
            return;
        }
        try {
            if (discoveryNodes.getLocalNode().isIngestNode()) {
                final SimulatePipelineRequest.Parsed simulateRequest;
                if (request.getId() != null) {
                    simulateRequest = SimulatePipelineRequest.parseWithPipelineId(
                        request.getId(),
                        source,
                        request.isVerbose(),
                        ingestService,
                        request.getRestApiVersion()
                    );
                } else {
                    simulateRequest = SimulatePipelineRequest.parse(
                        source,
                        request.isVerbose(),
                        ingestService,
                        request.getRestApiVersion()
                    );
                }
                executionService.execute(simulateRequest, listener);
            } else {
                DiscoveryNode ingestNode = getRandomIngestNode(ingestNodes.values());
                logger.trace("forwarding request [{}] to ingest node [{}]", actionName, ingestNode);
                ActionListenerResponseHandler<SimulatePipelineResponse> handler = new ActionListenerResponseHandler<>(
                    listener,
                    SimulatePipelineResponse::new,
                    TransportResponseHandler.TRANSPORT_WORKER
                );
                if (task == null) {
                    transportService.sendRequest(ingestNode, actionName, request, handler);
                } else {
                    transportService.sendChildRequest(
                        ingestNode,
                        actionName,
                        request,
                        task,
                        TransportRequestOptions.timeout(ingestNodeTransportActionTimeout),
                        handler
                    );
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private DiscoveryNode getRandomIngestNode(Collection<DiscoveryNode> ingestNodes) {
        return ingestNodes.toArray(new DiscoveryNode[0])[random.get().nextInt(ingestNodes.size())];
    }
}
