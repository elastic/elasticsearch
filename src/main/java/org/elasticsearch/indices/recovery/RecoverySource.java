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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class RecoverySource extends AbstractComponent {

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final ClusterService clusterService;

    private final TimeValue internalActionTimeout;
    private final TimeValue internalActionLongTimeout;


    @Inject
    public RecoverySource(Settings settings, TransportService transportService, IndicesService indicesService,
                          RecoverySettings recoverySettings, MappingUpdatedAction mappingUpdatedAction, ClusterService clusterService) {
        super(settings);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.clusterService = clusterService;

        this.recoverySettings = recoverySettings;

        transportService.registerHandler(Actions.START_RECOVERY, new StartRecoveryTransportRequestHandler());
        this.internalActionTimeout = componentSettings.getAsTime("internal_action_timeout", TimeValue.timeValueMinutes(15));
        this.internalActionLongTimeout = new TimeValue(internalActionTimeout.millis() * 2);
    }

    private RecoveryResponse recover(final StartRecoveryRequest request) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().index().name());
        final InternalIndexShard shard = (InternalIndexShard) indexService.shardSafe(request.shardId().id());

        // starting recovery from that our (the source) shard state is marking the shard to be in recovery mode as well, otherwise
        // the index operations will not be routed to it properly
        RoutingNode node = clusterService.state().readOnlyRoutingNodes().node(request.targetNode().id());
        if (node == null) {
            logger.debug("delaying recovery of {} as source node {} is unknown", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the node [" + request.targetNode() + "] in its state yet..");
        }
        ShardRouting targetShardRouting = null;
        for (ShardRouting shardRouting : node) {
            if (shardRouting.shardId().equals(request.shardId())) {
                targetShardRouting = shardRouting;
                break;
            }
        }
        if (targetShardRouting == null) {
            logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
        }
        if (!targetShardRouting.initializing()) {
            logger.debug("delaying recovery of {} as it is not listed as initializing on the target node {}. known shards state is [{}]",
                    request.shardId(), request.targetNode(), targetShardRouting.state());
            throw new DelayRecoveryException("source node has the state of the target shard to be [" + targetShardRouting.state() + "], expecting to be [initializing]");
        }

        logger.trace("[{}][{}] starting recovery to {}, mark_as_relocated {}", request.shardId().index().name(), request.shardId().id(), request.targetNode(), request.markAsRelocated());

        ShardRecoveryHandler handler = new ShardRecoveryHandler(shard, request, recoverySettings, transportService, internalActionTimeout,
                internalActionLongTimeout, clusterService, indicesService, mappingUpdatedAction, logger);

        shard.recover(handler);
        return handler.getResponse();
    }

    class StartRecoveryTransportRequestHandler extends BaseTransportRequestHandler<StartRecoveryRequest> {

        @Override
        public StartRecoveryRequest newInstance() {
            return new StartRecoveryRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }
}

