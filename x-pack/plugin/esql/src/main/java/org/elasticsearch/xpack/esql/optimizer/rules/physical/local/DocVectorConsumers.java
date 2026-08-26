/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Identifies expressions evaluated with a Lucene query evaluator, which requires a {@code _doc} input attribute. */
public final class DocVectorConsumers {

    private DocVectorConsumers() {}

    /** Returns whether {@code plan} has a non-runtime full-text function evaluated by Lucene and requiring {@code _doc}. */
    public static boolean consumesDocVector(PhysicalPlan plan) {
        Set<FullTextFunction> highlightQueryFunctions = Collections.newSetFromMap(new IdentityHashMap<>());
        if (plan instanceof HighlightExec highlight && highlight.query() != null) {
            // HIGHLIGHT translates its query into a MemoryIndex query, so it does not need _doc.
            highlight.query().forEachDown(FullTextFunction.class, highlightQueryFunctions::add);
        }
        Holder<Boolean> found = new Holder<>(false);
        plan.forEachExpression(FullTextFunction.class, ftf -> {
            if (found.get()) {
                return;
            }
            if (ftf.isRuntimeSearch() == false && highlightQueryFunctions.contains(ftf) == false) {
                found.set(true);
            }
        });
        return found.get();
    }
}
