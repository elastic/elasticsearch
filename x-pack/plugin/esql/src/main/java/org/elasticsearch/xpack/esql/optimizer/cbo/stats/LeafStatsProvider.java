/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

/** Supplies planning-safe metadata for leaves without evaluating their data sources. */
@FunctionalInterface
public interface LeafStatsProvider {
    /** Returns estimates from metadata already attached to a leaf plan. */
    PlanStats estimate(LeafPlan leaf);
}
