/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.existence;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This decider looks at all indices and ensures a minimum capacity is available if any indices are in the frozen ILM phase, since that
 * is designated for partially mounted indices/frozen tier only. Effectively, this scales the tier into existence.
 *
 * This works in concert with the `WaitForDataTierStep` in ILM that ensures we wait for autoscaling to spin up the first frozen tier node.
 */
public class FrozenExistenceDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "frozen_existence";
    static final ByteSizeValue MINIMUM_FROZEN_MEMORY = ByteSizeValue.ofGb(1);
    static final ByteSizeValue MINIMUM_FROZEN_STORAGE = ByteSizeValue.ofGb(8);

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        List<String> indicesNeedingFrozen = context.state()
            .metadata()
            .getProject()
            .stream()
            .filter(FrozenExistenceDeciderService::isFrozenPhase)
            .map(imd -> imd.getIndex().getName())
            .limit(10)
            .collect(Collectors.toList());
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder();
        if (indicesNeedingFrozen.size() > 0) {
            builder.total(MINIMUM_FROZEN_STORAGE, MINIMUM_FROZEN_MEMORY, null);
            builder.node(MINIMUM_FROZEN_STORAGE, MINIMUM_FROZEN_MEMORY, null);
        } else {
            builder.total(ByteSizeValue.ZERO, ByteSizeValue.ZERO, Processors.ZERO);
        }

        return new AutoscalingDeciderResult(builder.build(), new FrozenExistenceReason(indicesNeedingFrozen));
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return Collections.emptyList();
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    public static class FrozenExistenceReason implements AutoscalingDeciderResult.Reason {
        private final List<String> indices;

        public FrozenExistenceReason(List<String> indices) {
            this.indices = indices;
        }

        public FrozenExistenceReason(StreamInput in) throws IOException {
            this.indices = in.readStringCollectionAsList();
        }

        @Override
        public String summary() {
            return "indices " + indices;
        }

        public List<String> indices() {
            return indices;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("indices", indices);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FrozenExistenceReason that = (FrozenExistenceReason) o;
            return indices.equals(that.indices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices);
        }
    }

    // this is only here to support isFrozenPhase, TimeseriesLifecycleType.FROZEN_PHASE is the canonical source for this
    static String FROZEN_PHASE = "frozen"; // visible for testing

    /**
     * Return true if this index is in the frozen phase, false if not controlled by ILM or not in frozen.
     * @param indexMetadata the metadata of the index to retrieve phase from.
     * @return true if frozen phase, false otherwise.
     */
    // visible for testing
    static boolean isFrozenPhase(IndexMetadata indexMetadata) {
        return FROZEN_PHASE.equals(indexMetadata.getLifecycleExecutionState().phase());
    }
}
