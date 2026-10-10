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
 * Replaces the extraction of the {@code $$unmapped_fields} column of {@code SET unmapped_fields="LOAD_ALL"} with a
 * {@code null} when no shard on this node may hold an unmapped field (see {@code SearchStats#mayHoldUnmappedFields}).
 * <p>
 * Extracting the column reads and parses {@code _source} for every row, only to find nothing the mapping does not already
 * describe. The column itself stays in the plan, so the coordinator expands it into no column at all, as it does for any
 * row without unmapped fields.
 * <p>
 * Runs after {@code InsertFieldExtraction}, which is what adds the column to a {@link FieldExtractExec}.
 */
public class SkipUnmappedFieldsExtraction extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FieldExtractExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FieldExtractExec fieldExtractExec, LocalPhysicalOptimizerContext context) {
        List<Alias> nulls = new ArrayList<>();
        List<Attribute> remaining = new ArrayList<>(fieldExtractExec.attributesToExtract().size());
        for (Attribute attribute : fieldExtractExec.attributesToExtract()) {
            if (attribute instanceof UnmappedFieldsAttribute unmapped) {
                // Keeps the id, so that whatever refers to the extracted column now resolves to this one.
                nulls.add(new Alias(unmapped.source(), unmapped.name(), Literal.of(unmapped, null), unmapped.id()));
            } else {
                remaining.add(attribute);
            }
        }
        if (nulls.isEmpty() || context.searchStats().mayHoldUnmappedFields()) {
            return fieldExtractExec;
        }
        EvalExec eval = new EvalExec(fieldExtractExec.source(), fieldExtractExec.child(), nulls);
        // An extraction with nothing left to extract cannot be planned, and the child already supplies the rows.
        return remaining.isEmpty() ? eval : fieldExtractExec.withAttributesToExtract(remaining).replaceChild(eval);
    }
}
