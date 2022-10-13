/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.nodeinfo;

import org.elasticsearch.common.unit.Processors;

/**
 * Record for containing memory and processors for given node
 * @param memory node total memory
 * @param processors allocated processors
 */
public record AutoscalingNodeInfo(long memory, Processors processors) {

    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private Long memory;
        private Processors processors;

        Builder setMemory(long memory) {
            this.memory = memory;
            return this;
        }

        Builder setProcessors(double processors) {
            this.processors = Processors.of(processors);
            return this;
        }

        boolean canBuild() {
            return memory != null && processors != null;
        }

        AutoscalingNodeInfo build() {
            assert memory != null && processors != null : "unexpected null values when building node memory and processors information";
            return new AutoscalingNodeInfo(memory, processors);
        }
    }
}
