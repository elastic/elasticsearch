/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Warns when a {@code FILLNULL} without a {@code WITH} value targets a column whose type has no default fill value
 * (e.g. {@code date}, {@code date_nanos}, {@code ip}, {@code version}, geo types, or a genuinely {@code null}-typed
 * column). Such columns cannot be given a type default and are left unchanged; the user must supply a value with
 * {@code WITH}.
 * <p>
 * For an explicit field list, one warning is emitted per un-fillable field. For the all-fields form
 * ({@code FILLNULL} with no field list) a single summary warning lists the columns that were left unchanged, to avoid
 * flooding the response headers with one warning per column. That summary lists at most {@link #MAX_FIELDS_IN_WARNING}
 * field names; when more columns are left unchanged the message reports how many are shown so the header stays bounded.
 * <p>
 * This runs before {@link SubstituteSurrogatePlans} in {@code LogicalPlanOptimizer.substitutions()} because the
 * {@link FillNull} node is rewritten away by that substitution. When a {@code WITH} value is present there is nothing
 * to warn about: an incompatible explicitly-targeted field is a hard error and the all-fields form intentionally skips
 * incompatible columns.
 */
public final class WarnUnfillableFillNull extends Rule<LogicalPlan, LogicalPlan> {

    public static final int MAX_FIELDS_IN_WARNING = 10;

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        plan.forEachDown(FillNull.class, WarnUnfillableFillNull::warn);
        return plan;
    }

    private static void warn(FillNull fillNull) {
        // Only the type-default form (no WITH value) can leave a targeted column unfilled without it being an error.
        if (fillNull.fillValue() != null) {
            return;
        }
        List<Attribute> unfillable = fillNull.unfillableTargets();
        if (unfillable.isEmpty()) {
            return;
        }
        Source source = fillNull.source();
        int line = source.source().getLineNumber();
        int column = source.source().getColumnNumber();
        if (fillNull.targetFields().isEmpty()) {
            int shown = Math.min(unfillable.size(), MAX_FIELDS_IN_WARNING);
            StringJoiner names = new StringJoiner(", ");
            for (int i = 0; i < shown; i++) {
                names.add(unfillable.get(i).name());
            }
            if (unfillable.size() > MAX_FIELDS_IN_WARNING) {
                addWarning(
                    "Line {}:{}: [FILLNULL] the following fields have no default fill value for their type and were left "
                        + "unchanged: [{}]; provide a value using WITH; only the first {} of {} fields are shown",
                    line,
                    column,
                    names.toString(),
                    MAX_FIELDS_IN_WARNING,
                    unfillable.size()
                );
            } else {
                addWarning(
                    "Line {}:{}: [FILLNULL] the following fields have no default fill value for their type and were left "
                        + "unchanged: [{}]; provide a value using WITH",
                    line,
                    column,
                    names.toString()
                );
            }
        } else {
            for (Attribute field : unfillable) {
                addWarning(
                    "Line {}:{}: [FILLNULL] field [{}] of type [{}] has no default fill value and was left unchanged; "
                        + "provide a value using WITH",
                    line,
                    column,
                    field.name(),
                    field.dataType().typeName()
                );
            }
        }
    }
}
