/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/** Identifies expressions evaluated with a Lucene query evaluator, which requires a {@code _doc} input attribute. */
public final class DocVectorConsumers {

    private DocVectorConsumers() {}

    /** Returns whether {@code plan} contains a non-runtime full-text function, which requires {@code _doc}. */
    public static boolean consumesDocVector(PhysicalPlan plan) {
        Holder<Boolean> found = new Holder<>(false);
        plan.forEachExpression(Expression.class, e -> {
            if (found.get()) {
                return;
            }
            if (e instanceof FullTextFunction ftf && ftf.isRuntimeSearch() == false) {
                found.set(true);
            }
        });
        return found.get();
    }
}
