/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.core.ml.MlMetadata;

import java.util.OptionalDouble;

import static org.elasticsearch.xpack.ml.MachineLearning.CPU_RATIO_NODE_ATTR;

/**
 * Returns the node CPU taking into account node attributes and cluster state
 */
public final class NodeCpuRatio {

    /**
     *
     * @param state current cluster state
     * @param node Node whose ratio to return
     * @return The current ration if found
     */
    public static OptionalDouble nodeCpuRatio(ClusterState state, DiscoveryNode node) {
        String ratioAttr = node.getAttributes().get(CPU_RATIO_NODE_ATTR);
        if (ratioAttr != null) {
            try {
                return OptionalDouble.of(Double.parseDouble(ratioAttr));
            } catch (NumberFormatException ex) {
                assert false : "unable to parse ml.memory_cpu_ratio attribute [" + ratioAttr + "] " + ex.getMessage();
                return OptionalDouble.empty();
            }
        }
        double storedRatio = MlMetadata.getMlMetadata(state).getCpuRatio();
        return storedRatio > 0 ? OptionalDouble.of(storedRatio) : OptionalDouble.empty();
    }

}
