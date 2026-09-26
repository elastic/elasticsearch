/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Skips the {@code _source} read behind {@code SET unmapped_fields="LOAD_ALL"} on shards that provably hold no unmapped
 * field for the query's pattern.
 *
 * <p>{@code InsertFieldExtraction} adds the synthetic {@link UnmappedFieldsAttribute} to a {@link FieldExtractExec},
 * which at runtime turns into a per-document {@code _source} parse. When the data node's mapping guarantees that no
 * {@code _source} field could survive the attribute's pattern (see
 * {@code SearchStats#canSkipUnmappedFieldsExtraction}), that column is null in every row anyway. This rule replaces the
 * extraction with an {@link EvalExec} that assigns {@code null} to the same column, so the {@code _source} read never
 * happens.
 *
 * <p>The column stays in the plan (the coordinator's {@code ExpandUnmappedFieldsPostProcessor} still expands it to
 * nothing and drops it, exactly as it would for an all-null result), but the decision is now visible in the local
 * physical plan rather than hidden in the runtime operator, which is the natural place for a data node to make it.
 * Runs after {@code InsertFieldExtraction} so the {@link FieldExtractExec} it rewrites already exists.
 */
public class SkipUnmappedFieldsExtraction extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FieldExtractExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FieldExtractExec fieldExtractExec, LocalPhysicalOptimizerContext context) {
        List<Attribute> toExtract = fieldExtractExec.attributesToExtract();
        List<Alias> nullifiedColumns = new ArrayList<>();
        List<Attribute> remaining = new ArrayList<>(toExtract.size());
        for (Attribute attribute : toExtract) {
            if (attribute instanceof UnmappedFieldsAttribute unmapped
                && context.searchStats().canSkipUnmappedFieldsExtraction(unmapped.pattern())) {
                // Reuse the attribute's id so downstream references (and the exchange to the coordinator) resolve to the
                // null column produced here instead of the extracted one.
                nullifiedColumns.add(new Alias(unmapped.source(), unmapped.name(), Literal.of(unmapped, null), unmapped.id()));
            } else {
                remaining.add(attribute);
            }
        }
        if (nullifiedColumns.isEmpty()) {
            return fieldExtractExec;
        }
        EvalExec eval = new EvalExec(fieldExtractExec.source(), fieldExtractExec.child(), nullifiedColumns);
        return fieldExtractExec.withAttributesToExtract(remaining).replaceChild(eval);
    }
}
