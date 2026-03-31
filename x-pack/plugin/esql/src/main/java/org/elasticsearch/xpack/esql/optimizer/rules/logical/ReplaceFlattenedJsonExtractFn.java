/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.FlattenedEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

/**
 * Lowers {@link JsonExtract} on a mapped flattened root with a constant dotted path to a synthetic {@link FieldAttribute}
 * so {@link org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader} can load values directly.
 * <p>
 * The {@link EsqlCapabilities.Cap#JSON_EXTRACT_FLATTENED_FIELD} capability exists for CSV test gating only (see its Javadoc) —
 * it is not a product feature flag. When disabled, this rule is a no-op so planner behavior stays aligned with
 * {@link PropagateFlattened}. Other {@code JSON_EXTRACT} shapes keep parsing the root blob.
 */
public final class ReplaceFlattenedJsonExtractFn extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled() == false) {
            return plan;
        }
        return plan.transformExpressionsUp(JsonExtract.class, ReplaceFlattenedJsonExtractFn::replace);
    }

    private static Expression replace(JsonExtract extract) {
        Expression str = extract.children().get(0);
        Expression pathExpr = extract.children().get(1);
        if (str instanceof FieldAttribute fa && fa.field() instanceof FlattenedEsField) {
            if (pathExpr.foldable() == false) {
                return extract;
            }
            Object folded = pathExpr.fold(FoldContext.small());
            if (folded instanceof BytesRef br) {
                String pathStr = br.utf8ToString();
                if (FlattenedJsonExtractSupport.isKeyedFlattenedSubfieldPath(pathStr)) {
                    return FlattenedJsonExtractSupport.syntheticKeyedSubfield(extract.source(), fa, pathStr);
                }
            }
        }
        return extract;
    }
}
