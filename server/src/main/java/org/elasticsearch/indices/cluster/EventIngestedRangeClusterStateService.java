/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TODO LIST (Priority Order)
 *  ✓ Rearrange skeleton to have the Task submission only run on master
 *  ✓ Change code to properly screen for frozen shards only
 *  ✓ Determine how to update cluster state with the new index min/max ranges in TaskExecutor.execute
 *  ¤ Start writing UNIT tests for the above - are there similar unit tests for ShardStateAction.execute?
 *  ¤ Is there a way I can do manual testing of above?
 *  ¤
 *  ¤ Figure out what Writeable data structures need to go into the UpdateEventIngestedRangeRequest - implement that and manual testing
 *  ¤
 *  ¤ Design error handling and bwc - what if the master is older and doesn't have the transport action? (or any other TA error occurs)
 *  ¤ Fork shard min/max range lookups to background on data nodes?
 *  ¤ Pull TransportUpdateSettingsAction out to separate class file?
 *  ¤ Any basic unit tests to write? (compare to SnapshotsService and MetadataUpdateSettingsService)
 *  ¤ Start writing IT tests
 *  ¤ Deal with isDedicatedFrozenNode in the doStart check - remove this? Better way to detect if you have frozen indices?
 *  ¤
 *  ¤
 */

/**
 * TODO: DOCUMENT ME
 * Runs only on data nodes.
 * Sends event.ingested range to the master node via the TimestampRangeTransportAction
 */
// TODO: maybe rename to TimestampRangeClusterStateService ?
// TODO: other idea: add this functionality to IndicesClusterStateService?
// TODO: when can we remove this listener? when all frozen indices have complete event.ingested ranges in cluster state
// TODO: move to ClusterStateListener
public class EventIngestedRangeClusterStateService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(EventIngestedRangeClusterStateService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;

    // if true, attempt to lookup event.ingested ranges from searchable snapshots
    private boolean attemptEventIngestedRangeLookup;

    public EventIngestedRangeClusterStateService(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService
    ) {
        this(settings, clusterService, transportService, indicesService, false);
        logger.warn("XXX EventIngestedRangeClusterStateService constructor");
    }

    EventIngestedRangeClusterStateService(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        boolean attemptEventIngestedRangeLookup
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.attemptEventIngestedRangeLookup = attemptEventIngestedRangeLookup;
    }

    @Override
    protected void doStart() {
        logger.warn(
            "XXX EventIngestedRangeClusterStateService.doStart called: canContainData: {}; has frozen role?: {}; has cold role:? {}",
            DiscoveryNode.canContainData(settings),
            DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE),
            DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_COLD_NODE_ROLE)
        );

        // only run this service on data nodes that can have searchable snapshots mounted
        if (DiscoveryNode.canContainData(settings)
            && (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
                || DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_COLD_NODE_ROLE))) {
            logger.warn("XXX doStart 1");
            clusterService.addListener(this);
        } else {
            logger.warn("XXX doStart 2");
            clusterService.addListener(this);  // MP TODO: remove after manual testing done
        }
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() throws IOException {}

    // TODO: does this method need to be thread safe? Or is it guaranteed to be called serially
    // with a single thread as each ClusterChangedEvent occurs
    /**
     * Runs on data nodes.
     * If a cluster version upgrade is detected (e.g., moving from 8.15.0 to 8.16.0), then a Task
     * will be launched to determine whether any searchable snapshot shards "owned" by this
     * data node need to have their 'event.ingested' min/max range updated in cluster state.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        /// MP TODO ALT LOOPING MODEL -- start
        // for (Map.Entry<String, IndexMetadata> entry : event.state().getMetadata().indices().entrySet()) {
        // String indexName = entry.getKey();
        // IndexMetadata indexMetadata = entry.getValue();
        // Index index = entry.getValue().getIndex();
        //
        // if (isFrozenIndex(indexMetadata.getSettings())) {
        // IndexService indexService = indicesService.indexService(index);
        // for (Integer shardId : indexService.shardIds()) {
        // IndexShard shard = indexService.getShard(shardId);
        // ShardLongFieldRange eventIngestedRange = shard.getEventIngestedRange();
        // // TODO: add this range to map like below if we go this route
        // }
        //
        // }
        // }
        /// MP TODO ALT LOOPING MODEL -- end

        // only run this task when a cluster has upgraded to a new version
        // TODO: how is this going to work in serverless? will event.state().nodes().getMinNodeVersion() return useful info?
        if (clusterVersionUpgrade(event) || attemptEventIngestedRangeLookup) {
            Iterator<ShardRouting> shardRoutingIterator = event.state()
                .getRoutingNodes()
                .node(event.state().nodes().getLocalNodeId())
                .iterator();

            logger.warn(
                "XXX EventIngestedRangeClusterStateService.applyClusterState DEBUG 2. Created iterator: {}",
                shardRoutingIterator.hasNext()
            );

            List<ShardRouting> shardsForLookup = new ArrayList<>();
            while (shardRoutingIterator.hasNext()) {
                ShardRouting shardRouting = shardRoutingIterator.next();
                Settings indexSettings = event.state().metadata().index(shardRouting.index()).getSettings();
                if (isFrozenIndex(indexSettings)) {
                    shardsForLookup.add(shardRouting);
                }
            }

            logger.warn("XXX EventIngestedRangeClusterStateService.applyClusterState DEBUG 3. shardsForLookup: {}", shardsForLookup.size());

            Map<Index, List<ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();

            // MP TODO - bogus entry to have something for initial testing -- start
            // Index mpidx = new Index("mpidx", UUID.randomUUID().toString());
            // List<ShardRangeInfo> shardRangeList = new ArrayList<>();
            // shardRangeList.add(new ShardRangeInfo(new ShardId(mpidx, 22), ShardLongFieldRange.UNKNOWN));
            // eventIngestedRangeMap.put(mpidx, shardRangeList);
            // MP TODO - bogus entry to have something for initial testing -- end

            // TODO: does this min/max lookup logic need to be forked to background (if yes, how do I do that?)

            // TODO: create new list or map here of shards/indexes and new min/max range to update
            for (ShardRouting shardRouting : shardsForLookup) {
                IndexService indexService = indicesService.indexService(shardRouting.index());
                IndicesClusterStateService.Shard shard = indexService.getShardOrNull(shardRouting.shardId().id());
                // TODO: the above can return null - what do I do in that case?
                // TODO: ^^ speaks to the larger issue of error handling - what if on this pass not all the shards can't be
                // TODO: inspected for some reason - then the event.ingested range remains unusable until the next version upgrade?
                if (shard != null) {
                    IndexMetadata indexMetadata = event.state().metadata().index(shardRouting.index());
                    // TODO: is indexMetadata guaranteed to never be null here?
                    IndexLongFieldRange clusterStateEventIngestedRange = indexMetadata.getEventIngestedRange();

                    // MP TODO: is this the right check for whether to get min/max range from a shard?
                    if (shard.getEventIngestedRange() != ShardLongFieldRange.UNKNOWN
                        && clusterStateEventIngestedRange.containsAllShardRanges() == false) {
                        // MP TODO: it would be nice to be able to call getMax and getMin on the range in cluster state to avoid
                        // TODO unnecessary network calls,
                        // TODO but we aren't allowed to call those unless all shards are accounted for (so nothing left to update)

                        List<ShardRangeInfo> rangeInfoList = eventIngestedRangeMap.get(shardRouting.index());
                        if (rangeInfoList == null) {
                            rangeInfoList = new ArrayList<>();
                            eventIngestedRangeMap.put(shardRouting.index(), rangeInfoList);
                        }
                        rangeInfoList.add(new ShardRangeInfo(shard.shardId(), shard.getEventIngestedRange()));
                    }
                }
            }

            if (eventIngestedRangeMap.isEmpty() == false) {
                UpdateEventIngestedRangeRequest request = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);
                ActionListener<ActionResponse.Empty> execListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty response) {
                        try {
                            attemptEventIngestedRangeLookup = false;  // MP TODO: is this the right place to set this?
                            logger.warn("XXX YYY TaskExecutor.ActionListener onResponse: {}", response);
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    // TODO: what do we do on failure? Should we set a flag in the service stating that next time
                    // TODO: clusterChanged is called to try again regardless of whether there was a cluster version upgrade?
                    @Override
                    public void onFailure(Exception e) {
                        // attempt failed, so set a flag to retry on the next ClusterChangedEvent
                        attemptEventIngestedRangeLookup = true;
                        logger.warn("XXX YYY TaskExecutor.ActionListener onFailure: {}", e.getMessage());
                    }

                    @Override
                    public String toString() {
                        return "My temp exec listener in TaskExecutor";
                    }
                };

                logger.warn(
                    "XXX About to send to Master {}. request: {}",
                    UpdateEventIngestedRangeTransportAction.UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME,
                    request
                );
                // MP TODO: need to add tests around the Transport Service
                transportService.sendRequest(
                    transportService.getLocalNode(),
                    UpdateEventIngestedRangeTransportAction.UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME,
                    request,
                    // TODO: do I need something better than this response handler?
                    new ActionListenerResponseHandler<>(
                        execListener.safeMap(r -> null),  // TODO: I think this will call my onResponse and onFailure above?
                        AcknowledgedResponse::readFrom,
                        TransportResponseHandler.TRANSPORT_WORKER
                    )
                );
            }
        }
    }

    /**
     * Determine whether a cluster version upgrade has just happened.
     */
    private boolean clusterVersionUpgrade(ClusterChangedEvent event) {
        logger.warn("XXX clusterVersionUpgrade: event.nodesChanged(): " + event.nodesChanged());
        logger.warn("XXX clusterVersionUpgrade: event.state().nodes().getMinNodeVersion(): " + event.state().nodes().getMinNodeVersion());
        logger.warn("XXX clusterVersionUpgrade: event.state().nodes().getMaxNodeVersion(): " + event.state().nodes().getMaxNodeVersion());
        // return event.nodesChanged() &&
        // event.state().nodes().getMinNodeVersion() == event.state().nodes().getMaxNodeVersion() &&
        // event.previousState().nodes().getMinNodeVersion() == event.state().nodes().getMinNodeVersion();
        return event.nodesChanged() || event.nodesRemoved();  // MP FIXME change to commented out logic above
    }

    // TODO: copied from FrozenUtils - should that one move to core/server rather than be in xpack?
    public static boolean isFrozenIndex(Settings indexSettings) {
        return true;  // MP FIXME - change to commented out logic below
        // String tierPreference = DataTier.TIER_PREFERENCE_SETTING.get(indexSettings);
        // List<String> preferredTiers = DataTier.parseTierList(tierPreference);
        // if (preferredTiers.isEmpty() == false && preferredTiers.get(0).equals(DataTier.DATA_FROZEN)) {
        // assert preferredTiers.size() == 1 : "frozen tier preference must be frozen only";
        // return true;
        // } else {
        // return false;
        // }
    }

    public static class ShardRangeInfo extends TransportRequest {
        final ShardId shardId;
        final ShardLongFieldRange eventIngestedRange;

        ShardRangeInfo(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.eventIngestedRange = ShardLongFieldRange.readFrom(in);
        }

        public ShardRangeInfo(ShardId shardId, ShardLongFieldRange eventIngestedRange) {
            this.shardId = shardId;
            this.eventIngestedRange = eventIngestedRange;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            eventIngestedRange.writeTo(out);
        }

        @Override
        public String toString() {
            return Strings.format(
                "EventIngestedRangeService.ShardRangeInfo{shardId [%s], eventIngestedRange [%s]}",
                shardId,
                eventIngestedRange
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardRangeInfo that = (ShardRangeInfo) o;
            return shardId.equals(that.shardId) && eventIngestedRange.equals(that.eventIngestedRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, eventIngestedRange);
        }
    }
}
