/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.autoscaling.LocalStateAutoscaling;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;

import java.util.ArrayList;
import java.util.Collection;

public abstract class AutoscalingStorageIntegTestCase extends DiskUsageIntegTestCase {
    protected static final long HIGH_WATERMARK_BYTES = 10240;
    protected static final long LOW_WATERMARK_BYTES = 2 * HIGH_WATERMARK_BYTES;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LocalStateAutoscaling.class);
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), LOW_WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), HIGH_WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), "true");
        return builder.build();
    }

    public void setTotalSpace(String dataNodeName, long totalSpace) {
        getTestFileStore(dataNodeName).setTotalSpace(totalSpace);
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
    }

    public GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        GetAutoscalingCapacityAction.Response response = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        return response;
    }
}
