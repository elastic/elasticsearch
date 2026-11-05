/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.ExchangeRef;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;

import java.util.List;

/**
 * This class is {@code public} for testing.
 * @param nodeReducePlan The plan to be executed on the node_reduce driver. This should <i>not</i> contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}, but be a plan "sandwiched" between an {@link ExchangeSinkExec} and an
 * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}.
 * @param dataNodePlan The plan to be executed on the data driver. This may contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}.
 */
public record ReductionPlan(ExchangeSinkExec nodeReducePlan, ExchangeSinkExec dataNodePlan) {

    public boolean isPassThrough() {
        return nodeReducePlan.child() instanceof ExchangeSourceExec;
    }

    /**
     * Names the hop between the data driver and the node reduce driver, which both run on this node. Without it the data sink would
     * carry the name of the hop to the coordinator, as if the node reduce driver were not there. The sink of the node reduce plan
     * keeps the original name, because it is what feeds the coordinator.
     * <p>
     * The data plan is copied first. It can be the request's own plan, which other requests on this node may still be running.
     */
    public ReductionPlan withLocalExchange() {
        ExchangeRef local = ExchangeRef.newExchange();
        ExchangeSinkExec data = (ExchangeSinkExec) dataNodePlan.replaceChildren(List.of(dataNodePlan.child()));
        data.withExchange(local);
        nodeReducePlan.forEachDown(ExchangeSourceExec.class, source -> source.withExchange(local));
        return new ReductionPlan(nodeReducePlan, data);
    }
}
