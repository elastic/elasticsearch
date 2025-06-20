/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.extra;

import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.BiConsumer;

public class DisallowCategorize implements Verifier.ExtraCheckers {
    @Override
    public List<BiConsumer<LogicalPlan, Failures>> extra() {
        return List.of(DisallowCategorize::disallowCategorize);
    }

    private static void disallowCategorize(LogicalPlan plan, Failures failures) {
        if (plan instanceof Aggregate) {
            plan.forEachExpression(Categorize.class, cat -> failures.add(new Failure(cat, "CATEGORIZE is unsupported")));
        }
    }
}
