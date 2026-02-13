/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;

/**
 * This class is {@code public} for testing.
 * @param nodeReducePlan The plan to be executed on the node_reduce driver. This should <i>not</i> contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}, but be a plan "sandwiched" between an {@link ExchangeSinkExec} and an
 * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}.
 * @param dataNodePlan The plan to be executed on the data driver. This may contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}.
 */
public record ReductionPlan(
    ExchangeSinkExec nodeReducePlan,
    ExchangeSinkExec dataNodePlan,
    // TODO This should always be DISABLED; see https://github.com/elastic/elasticsearch/issues/142392.
    LocalPhysicalOptimization localPhysicalOptimization
) {
    public ReductionPlan withoutLocalPhysicalOptimization() {
        return new ReductionPlan(nodeReducePlan, dataNodePlan, LocalPhysicalOptimization.DISABLED);
    }
}
