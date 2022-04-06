/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoscalingCountTestDeciderService implements AutoscalingDeciderService {

    public static final String NAME = "count";

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        return new AutoscalingDeciderResult(null, new CountReason(counter.incrementAndGet()));
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of();
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return DiscoveryNodeRole.roles().stream().toList();
    }

    @Override
    public boolean defaultOn() {
        return false;
    }

    public static class CountReason implements AutoscalingDeciderResult.Reason {
        private final int count;

        public CountReason(int count) {
            this.count = count;
        }

        public CountReason(StreamInput in) throws IOException {
            this.count = in.readInt();
        }

        @Override
        public String summary() {
            return Integer.toString(count);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", count);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountReason that = (CountReason) o;
            return count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count);
        }
    }
}
