/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.Set;

/** Shared eligibility checks for hash-shuffle aggregation and cost-based alternatives. */
public final class ShuffleRuleUtils {
    private static final Set<ElementType> HASH_KEY_TYPES = Set.of(
        ElementType.INT,
        ElementType.LONG,
        ElementType.DOUBLE,
        ElementType.BOOLEAN,
        ElementType.BYTES_REF,
        ElementType.NULL
    );

    private ShuffleRuleUtils() {}

    /** Checks that groupings are non-empty and can be used as shuffle keys. */
    public static boolean hasSupportedGroupings(List<? extends Expression> groupings) {
        return groupings.isEmpty() == false && hasSupportedGroupKeyTypes(groupings);
    }

    /** Checks whether each grouping expression can be represented by the hash exchange. */
    public static boolean hasSupportedGroupKeyTypes(List<? extends Expression> groupings) {
        for (Expression grouping : groupings) {
            if (grouping.anyMatch(GroupingFunction.NonEvaluatableGroupingFunction.class::isInstance)) {
                return false;
            }
            try {
                if (HASH_KEY_TYPES.contains(PlannerUtils.toElementType(grouping.dataType())) == false) {
                    return false;
                }
            } catch (EsqlIllegalArgumentException e) {
                return false;
            }
        }
        return true;
    }
}
