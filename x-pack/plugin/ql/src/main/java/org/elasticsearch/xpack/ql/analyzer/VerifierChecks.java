/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Set;

import static org.elasticsearch.xpack.ql.common.Failure.fail;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;

public final class VerifierChecks {

    public static void checkFilterConditionType(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof Filter) {
            Expression condition = ((Filter) p).condition();
            if (condition.dataType() != BOOLEAN) {
                localFailures.add(fail(condition, "Condition expression needs to be boolean, found [{}]", condition.dataType()));
            }
        }
    }

}
