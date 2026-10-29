/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Keeps shuffle planning away from plan shapes whose execution paths are not handled by the stage scheduler.
 */
public final class ShuffleGate {

    private ShuffleGate() {}

    public static boolean allowed(Configuration configuration, LogicalPlan plan) {
        return configuration.pragmas().shuffleEnabled() && plan.anyMatch(ShuffleGate::unsupportedLogicalNode) == false;
    }

    public static boolean allowed(Configuration configuration, PhysicalPlan plan) {
        return configuration.pragmas().shuffleEnabled()
            && plan.anyMatch(
                node -> node instanceof MergeExec merge && merge.kind() == MergeExec.Kind.FORK
                    || node instanceof FragmentExec fragment && fragment.fragment().anyMatch(ShuffleGate::unsupportedLogicalNode)
            ) == false;
    }

    private static boolean unsupportedLogicalNode(LogicalPlan plan) {
        return plan instanceof Fork
            || plan instanceof TimeSeriesAggregate
            || plan instanceof EsRelation relation
                && relation.concreteIndices().keySet().stream().anyMatch(alias -> alias.isEmpty() == false);
    }

}
