/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.Processors;

public record MlProcessorAutoscalingCapacity(Processors nodeProcessors, Processors tierProcessors, String reason) {

    public static Builder builder(Processors nodeProcessors, Processors tierProcessors) {
        return new Builder(nodeProcessors, tierProcessors);
    }

    @Override
    public String toString() {
        return "MlProcessorAutoscalingCapacity{"
            + "nodeProcessors="
            + nodeProcessors
            + ", tierProcessors="
            + tierProcessors
            + ", reason='"
            + reason
            + '\''
            + '}';
    }

    public static class Builder {

        private Processors nodeProcessors;
        private Processors tierProcessors;
        private String reason;

        public Builder(Processors nodeProcessors, Processors tierProcessors) {
            this.nodeProcessors = nodeProcessors;
            this.tierProcessors = tierProcessors;
        }

        public Builder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        MlProcessorAutoscalingCapacity build() {
            return new MlProcessorAutoscalingCapacity(nodeProcessors, tierProcessors, reason);
        }
    }
}
