/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;

public record MlMemoryAutoscalingCapacity(ByteSizeValue nodeSize, ByteSizeValue tierSize, String reason) {

    public static Builder builder(ByteSizeValue nodeSize, ByteSizeValue tierSize) {
        return new Builder(nodeSize, tierSize);
    }

    public static Builder from(AutoscalingCapacity autoscalingCapacity) {
        if (autoscalingCapacity == null) {
            return builder(null, null);
        } else {
            return builder(autoscalingCapacity.node().memory(), autoscalingCapacity.total().memory());
        }
    }

    @Override
    public String toString() {
        return "MlMemoryAutoscalingCapacity{" + "nodeSize=" + nodeSize + ", tierSize=" + tierSize + ", reason='" + reason + '\'' + '}';
    }

    public boolean isUndetermined() {
        return nodeSize == null && tierSize == null;
    }

    public static class Builder {

        private ByteSizeValue nodeSize;
        private ByteSizeValue tierSize;
        private String reason;

        public Builder(ByteSizeValue nodeSize, ByteSizeValue tierSize) {
            assert (nodeSize == null) == (tierSize == null) : "nodeSize " + nodeSize + " tierSize " + tierSize;
            this.nodeSize = nodeSize;
            this.tierSize = tierSize;
        }

        public Builder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        public MlMemoryAutoscalingCapacity build() {
            return new MlMemoryAutoscalingCapacity(nodeSize, tierSize, reason);
        }
    }
}
