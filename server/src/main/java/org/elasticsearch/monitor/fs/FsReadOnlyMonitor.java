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

package org.elasticsearch.monitor.fs;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Monitor runs on master and listens for events from #ClusterInfoService on node stats. It checks to see if
 * a node has all paths writable if not removes the node from the cluster based on the setting monitor.fs.unhealthy.remove_enabled
 */
public class FsReadOnlyMonitor {

    private static final Logger logger = LogManager.getLogger(FsReadOnlyMonitor.class);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();
    private volatile boolean nodeRemovalEnabled;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final BiConsumer<DiscoveryNode, String> removeNode;
    private final Supplier<DiscoveryNode> localNodeSupplier;

    public static final Setting<Boolean> REMOVE_UNHEALTHY_FS_ENABLED_SETTING =
        Setting.boolSetting("monitor.fs.unhealthy.remove_enabled", true, Setting.Property.Dynamic,
            Setting.Property.NodeScope);


    public FsReadOnlyMonitor(Settings settings, ClusterSettings clusterSettings, Supplier<ClusterState> clusterStateSupplier,
                             Supplier<DiscoveryNode> localNodeSupplier, BiConsumer<DiscoveryNode, String> removeNode,
                             ClusterInfoService clusterInfoService) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.removeNode = removeNode;
        this.localNodeSupplier = localNodeSupplier;
        this.nodeRemovalEnabled = REMOVE_UNHEALTHY_FS_ENABLED_SETTING.get(settings);
        clusterInfoService.addListener(this::onNewInfo);
        clusterSettings.addSettingsUpdateConsumer(REMOVE_UNHEALTHY_FS_ENABLED_SETTING, this::setEnabled);
    }

    private void setEnabled(boolean nodeRemovalEnabled){
        this.nodeRemovalEnabled = nodeRemovalEnabled;
    }

    public void onNewInfo(ClusterInfo info) {
        if (checkInProgress.compareAndSet(false, true) == false) {
            logger.info("Skipping FS readonly monitor as a check is already in progress");
            return;
        }
        ImmutableOpenMap<String, Boolean> allPathsWritable = info.getNodeAllPathsWritable();
        final ClusterState state = clusterStateSupplier.get();
        DiscoveryNodes discoveryNodes = state.nodes();
        if(allPathsWritable == null)
            return;
        for (final ObjectObjectCursor<String, Boolean> entry : allPathsWritable) {
            final String node = entry.key;
            final Boolean allPathWritableForNode = entry.value;
            final DiscoveryNode discoveryNode = discoveryNodes.get(node);
            if(allPathWritableForNode == Boolean.FALSE){
                if (discoveryNode != localNodeSupplier.get()){
                    checkAndRemove(discoveryNode);
                }
            }
        }
        checkFinished();
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
    }

    private void checkAndRemove(DiscoveryNode discoveryNode){
        if(nodeRemovalEnabled)
            removeNode.accept(discoveryNode, "read-only-filesystem");
    }
}
