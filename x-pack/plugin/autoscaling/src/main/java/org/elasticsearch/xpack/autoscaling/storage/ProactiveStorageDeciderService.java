/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ProactiveStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "proactive_storage";
    public static final Setting<TimeValue> FORECAST_WINDOW = Setting.timeSetting("forecast_window", TimeValue.timeValueMinutes(30));

    private final DiskThresholdSettings diskThresholdSettings;
    private final AllocationDeciders allocationDeciders;
    private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;

    public ProactiveStorageDeciderService(
        Settings settings,
        ClusterSettings clusterSettings,
        AllocationDeciders allocationDeciders,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.allocationDeciders = allocationDeciders;
        this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.total().storage() == null) {
            return new AutoscalingDeciderResult(
                null,
                new ReactiveStorageDeciderService.ReactiveReason("current capacity not available", -1, -1)
            );
        }

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            context,
            diskThresholdSettings,
            allocationDeciders,
            shardRoutingRoleStrategy

        );
        long unassignedBytesBeforeForecast = allocationState.storagePreventsAllocation().sizeInBytes();
        assert unassignedBytesBeforeForecast >= 0;

        TimeValue forecastWindow = FORECAST_WINDOW.get(configuration);
        ReactiveStorageDeciderService.AllocationState allocationStateAfterForecast = allocationState.forecast(
            forecastWindow.millis(),
            System.currentTimeMillis()
        );

        long unassignedBytes = allocationStateAfterForecast.storagePreventsAllocation().sizeInBytes();
        long assignedBytes = allocationStateAfterForecast.storagePreventsRemainOrMove().sizeInBytes();
        long maxShardSize = allocationStateAfterForecast.maxShardSize();
        assert assignedBytes >= 0;
        assert unassignedBytes >= unassignedBytesBeforeForecast;
        assert maxShardSize >= 0;
        String message = ReactiveStorageDeciderService.message(unassignedBytes, assignedBytes);
        AutoscalingCapacity requiredCapacity = AutoscalingCapacity.builder()
            .total(autoscalingCapacity.total().storage().getBytes() + unassignedBytes + assignedBytes, null, null)
            .node(maxShardSize, null, null)
            .build();
        return new AutoscalingDeciderResult(
            requiredCapacity,
            new ProactiveReason(message, unassignedBytes, assignedBytes, unassignedBytes - unassignedBytesBeforeForecast, forecastWindow)
        );
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(FORECAST_WINDOW);
    }

    public static class ProactiveReason implements AutoscalingDeciderResult.Reason {
        private final String reason;
        private final long unassigned;
        private final long assigned;
        private final long forecasted;
        private final TimeValue forecastWindow;

        public ProactiveReason(String reason, long unassigned, long assigned, long forecasted, TimeValue forecastWindow) {
            this.reason = reason;
            this.unassigned = unassigned;
            this.assigned = assigned;
            this.forecasted = forecasted;
            this.forecastWindow = forecastWindow;
        }

        public ProactiveReason(StreamInput in) throws IOException {
            this.reason = in.readString();
            this.unassigned = in.readLong();
            this.assigned = in.readLong();
            this.forecasted = in.readLong();
            this.forecastWindow = in.readTimeValue();
        }

        @Override
        public String summary() {
            return reason;
        }

        public long unassigned() {
            return unassigned;
        }

        public long assigned() {
            return assigned;
        }

        public long forecasted() {
            return forecasted;
        }

        public TimeValue forecastWindow() {
            return forecastWindow;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
            out.writeLong(unassigned);
            out.writeLong(assigned);
            out.writeLong(forecasted);
            out.writeTimeValue(forecastWindow);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.field("unassigned", unassigned);
            builder.field("assigned", assigned);
            builder.field("forecasted", forecasted);
            builder.field("forecast_window", forecastWindow);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProactiveReason that = (ProactiveReason) o;
            return unassigned == that.unassigned
                && assigned == that.assigned
                && forecasted == that.forecasted
                && reason.equals(that.reason)
                && forecastWindow.equals(that.forecastWindow);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, unassigned, assigned, forecasted, forecastWindow);
        }
    }
}
