/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

/** Selects whether a join should use a shuffle exchange or an existing join path. */
public final class JoinStrategySelector {
    /** Available physical strategies for logical joins. */
    public enum JoinStrategy {
        PARTITIONED,
        BROADCAST,
        LOOKUP,
        MATERIALIZED,
        LOCAL
    }

    private JoinStrategySelector() {}

    /** Selects the legacy strategy used when the cost-based planner has no applicable decision. */
    public static JoinStrategy select(Join join, QueryPragmas pragmas, boolean leftIsScan) {
        if (join instanceof SubqueryLookupJoin) {
            if (Mapper.distributedJoinEligible(join) == false || pragmas.shuffleJoin() == false) {
                return JoinStrategy.MATERIALIZED;
            }
            return switch (pragmas.shuffleJoinStrategy()) {
                case "partitioned" -> JoinStrategy.PARTITIONED;
                case "broadcast" -> JoinStrategy.BROADCAST;
                default -> throw new EsqlIllegalArgumentException(
                    "unsupported [shuffle_join_strategy] value ["
                        + pragmas.shuffleJoinStrategy()
                        + "]; expected [partitioned] or [broadcast]"
                );
            };
        }
        if (isLookupIndexJoin(join)) {
            return pragmas.shuffleJoin() && Mapper.distributedJoinEligible(join) && leftIsScan
                ? JoinStrategy.PARTITIONED
                : JoinStrategy.LOOKUP;
        }
        return JoinStrategy.LOCAL;
    }

    /** Identifies joins whose right side is a lookup-mode index. */
    public static boolean isLookupIndexJoin(Join join) {
        return Mapper.isIndexModeLookup(join.right());
    }
}
