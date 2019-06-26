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

package org.elasticsearch.cluster.routing.allocation;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

/**
 * Listens for a node to go over the high watermark and kicks off an empty
 * reroute if it does. Also responsible for logging about nodes that have
 * passed the disk watermarks
 */
public class DiskThresholdMonitor {

    private static final Logger logger = LogManager.getLogger(DiskThresholdMonitor.class);

    private final DiskThresholdSettings diskThresholdSettings;
    private final Client client;
    private final Set<String> nodeHasPassedWatermark = Sets.newConcurrentHashSet();
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final AtomicLong lastRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();
    private final SetOnce<Consumer<ActionListener<Void>>> rerouteAction = new SetOnce<>();

    public DiskThresholdMonitor(Settings settings, Supplier<ClusterState> clusterStateSupplier, ClusterSettings clusterSettings,
                                Client client, LongSupplier currentTimeMillisSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.client = client;
    }

    /**
     * Warn about the given disk usage if the low or high watermark has been passed
     */
    private void warnAboutDiskIfNeeded(DiskUsage usage) {
        // Check absolute disk values
        if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes()) {
            logger.warn("flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                diskThresholdSettings.getFreeBytesThresholdFloodStage(), usage);
        } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            logger.warn("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node",
                diskThresholdSettings.getFreeBytesThresholdHigh(), usage);
        } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()) {
            logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                diskThresholdSettings.getFreeBytesThresholdLow(), usage);
        }

        // Check percentage disk values
        if (usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage()) {
            logger.warn("flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                Strings.format1Decimals(100.0 - diskThresholdSettings.getFreeDiskThresholdFloodStage(), "%"), usage);
        } else if (usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            logger.warn("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node",
                Strings.format1Decimals(100.0 - diskThresholdSettings.getFreeDiskThresholdHigh(), "%"), usage);
        } else if (usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdLow()) {
            logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                Strings.format1Decimals(100.0 - diskThresholdSettings.getFreeDiskThresholdLow(), "%"), usage);
        }
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
    }

    public void onNewInfo(ClusterInfo info) {

        assert rerouteAction.get() != null;

        if (checkInProgress.compareAndSet(false, true) == false) {
            logger.info("skipping monitor as a check is already in progress");
            return;
        }

        final ImmutableOpenMap<String, DiskUsage> usages = info.getNodeLeastAvailableDiskUsages();
        if (usages == null) {
            checkFinished();
            return;
        }

        boolean reroute = false;
        String explanation = "";
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

        // Garbage collect nodes that have been removed from the cluster
        // from the map that tracks watermark crossing
        final ObjectLookupContainer<String> nodes = usages.keys();
        for (String node : nodeHasPassedWatermark) {
            if (nodes.contains(node) == false) {
                nodeHasPassedWatermark.remove(node);
            }
        }
        final ClusterState state = clusterStateSupplier.get();
        final Set<String> indicesToMarkReadOnly = new HashSet<>();

        for (final ObjectObjectCursor<String, DiskUsage> entry : usages) {
            final String node = entry.key;
            final DiskUsage usage = entry.value;
            warnAboutDiskIfNeeded(usage);
            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage()) {
                final RoutingNode routingNode = state.getRoutingNodes().node(node);
                if (routingNode != null) { // this might happen if we haven't got the full cluster-state yet?!
                    for (ShardRouting routing : routingNode) {
                        indicesToMarkReadOnly.add(routing.index().getName());
                    }
                }
            } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {
                if (lastRunTimeMillis.get() < currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                    reroute = true;
                    explanation = "high disk watermark exceeded on one or more nodes";
                } else {
                    logger.debug("high disk watermark exceeded on {} but an automatic reroute has occurred " +
                            "in the last [{}], skipping reroute",
                        node, diskThresholdSettings.getRerouteInterval());
                }
                nodeHasPassedWatermark.add(node);
            } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLow().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdLow()) {
                nodeHasPassedWatermark.add(node);
            } else {
                if (nodeHasPassedWatermark.contains(node)) {
                    // The node has previously been over the high or
                    // low watermark, but is no longer, so we should
                    // reroute so any unassigned shards can be allocated
                    // if they are able to be
                    if (lastRunTimeMillis.get() < currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                        reroute = true;
                        explanation = "one or more nodes has gone under the high or low watermark";
                        nodeHasPassedWatermark.remove(node);
                    } else {
                        logger.debug("{} has gone below a disk threshold, but an automatic reroute has occurred " +
                                "in the last [{}], skipping reroute",
                            node, diskThresholdSettings.getRerouteInterval());
                    }
                }
            }
        }

        final ActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(this::checkFinished), 2);
        final ActionListener<Void> timeUpdatingListener = ActionListener.wrap(r -> {
            lastRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
            listener.onResponse(r);
        }, listener::onFailure);

        if (reroute) {
            logger.info("rerouting shards: [{}]", explanation);
            rerouteAction.get().accept(timeUpdatingListener);
        } else {
            listener.onResponse(null);
        }
        indicesToMarkReadOnly.removeIf(index -> state.getBlocks().indexBlocked(ClusterBlockLevel.WRITE, index));
        if (indicesToMarkReadOnly.isEmpty() == false) {
            markIndicesReadOnly(indicesToMarkReadOnly, timeUpdatingListener);
        } else {
            listener.onResponse(null);
        }
    }

    protected void markIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener) {
        // set read-only block but don't block on the response
        client.admin().indices().prepareUpdateSettings(indicesToMarkReadOnly.toArray(Strings.EMPTY_ARRAY))
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE, true).build())
            .execute(ActionListener.map(listener, r -> null));
    }

    public void setRerouteAction(BiConsumer<String, ActionListener<Void>> rerouteAction) {
        this.rerouteAction.set(listener -> rerouteAction.accept("disk threshold monitor", listener));
    }
}
