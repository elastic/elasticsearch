/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Map;

/**
 * Grafts the out-of-band request {@code filter} onto external-source (dataset) leaves of an analyzed plan.
 *
 * <p>For each {@link ExternalRelation}, the filter is translated against that relation's exposed schema (a present
 * field binds to its attribute, a missing field to {@link Literal#NULL}) and wrapped as an ordinary {@link Filter}
 * above the relation. From there the existing optimizer pushes it down and the engine evaluates it — the grafted
 * filter is indistinguishable from a user-written {@code WHERE}. Index leaves keep their existing path and are not
 * touched here.
 *
 * <p>A construct outside the supported subset raises {@link TranslationUnsupportedException}; consistent with the
 * leniency contract, the request filter degrades that clause to a per-source no-op (the relation is left unfiltered).
 */
public final class RequestFilterGraft {

    private RequestFilterGraft() {}

    /**
     * @param nowInMillis the query's start time, epoch millis — anchors {@code now} date math so a request filter over
     *                    an external source resolves {@code "now-15m"} to the same instant the index path would.
     */
    public static LogicalPlan graft(LogicalPlan analyzed, QueryBuilder requestFilter, long nowInMillis) {
        if (requestFilter == null) {
            return analyzed;
        }
        LogicalPlan grafted = analyzed.transformUp(ExternalRelation.class, relation -> {
            Map<String, Attribute> byName = new HashMap<>();
            for (Attribute a : relation.output()) {
                byName.put(a.name(), a);
            }
            QueryDslTranslator translator = new QueryDslTranslator(name -> {
                Attribute a = byName.get(name);
                return a != null ? a : Literal.NULL;
            }, nowInMillis);
            try {
                Expression condition = translator.translate(requestFilter);
                return new Filter(relation.source(), relation, condition);
            } catch (TranslationUnsupportedException e) {
                // Per-source degrade: an untranslatable clause is not applied to this source rather than failing.
                return relation;
            }
        });
        // The grafted Filter and the rebuilt spine above it are fresh nodes at stage NEW; the plan was already
        // analyzed, so mark the (idempotent for unchanged nodes) tree analyzed to satisfy the pre-optimizer.
        grafted.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return grafted;
    }
}
