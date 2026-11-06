/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;

/** Renders stage edges without including node ids or other runtime-specific metadata. */
final class StagedPlanShape {
    private StagedPlanShape() {}

    static String of(StagedPlanner.StagedPlan plan) {
        StringBuilder result = new StringBuilder();
        for (StagedPlanner.Stage stage : plan.stages()) {
            result.append('S').append(stage.id()).append(' ').append(stage.placement());
            if (stage.inputEdges().isEmpty() == false) {
                result.append(" in=").append(stage.inputEdges());
            }
            if (stage.outputEdge() != null) {
                result.append(" out=").append(stage.outputEdge());
            }
            result.append('\n');
        }
        for (StagedPlanner.Edge edge : plan.edges()) {
            result.append(edge.edgeKey()).append(" [");
            for (int i = 0; i < edge.producerStages().size(); i++) {
                if (i > 0) {
                    result.append(", ");
                }
                result.append('S').append(edge.producerStages().get(i));
            }
            result.append("] -> S").append(edge.consumerStage()).append(' ').append(distribution(edge.distribution())).append('\n');
        }
        return result.toString();
    }

    private static String distribution(ExchangeDistribution distribution) {
        return switch (distribution) {
            case GATHER -> "GATHER";
            case HASH -> "HASH";
            case BROADCAST -> "BROADCAST";
        };
    }
}
