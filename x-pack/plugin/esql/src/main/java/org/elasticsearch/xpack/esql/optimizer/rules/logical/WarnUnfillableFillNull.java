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
 * Reports, as a response-header warning, every targeted column that {@code FILLNULL} left unchanged.
 * <p>
 * {@code FILLNULL} never fails over a column it cannot fill - not for a type mismatch, a value out of range for the
 * column, a string that will not parse into a date / ip / version, a {@code null}-typed column, nor a type that has no
 * default under {@code DEFAULT}. The outcome no longer depends on how the column was selected: naming a column, matching
 * it with a pattern and sweeping it up with {@code *} all leave it unchanged and warn. The column keeps whatever it had,
 * null or not.
 * <p>
 * Always a <em>single</em> summary warning listing the columns, whatever the target form, so the response headers stay
 * bounded and the shape is predictable. It lists at most {@link #MAX_FIELDS_IN_WARNING} names and then reports how many
 * of how many are shown.
 * <p>
 * The one silent form is an explicit {@code NULL} value, which means "do not fill": nothing is unexpectedly left
 * unchanged, so there is nothing to report.
 * <p>
 * This runs before {@link SubstituteSurrogatePlans} in {@code LogicalPlanOptimizer.substitutions()} because that
 * substitution rewrites the {@link FillNull} node away.
 */
public final class WarnUnfillableFillNull extends Rule<LogicalPlan, LogicalPlan> {

    public static final int MAX_FIELDS_IN_WARNING = 10;

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        plan.forEachDown(FillNull.class, WarnUnfillableFillNull::warn);
        return plan;
    }

    private static void warn(FillNull fillNull) {
        // An explicit NULL is a deliberate no-op, so nothing was unexpectedly left unchanged.
        if (fillNull.isExplicitNullFill()) {
            return;
        }
        Source source = fillNull.source();
        int line = source.source().getLineNumber();
        int column = source.source().getColumnNumber();

        // A multi-valued value (only reachable through a list-valued ?param) fills nothing at all. Reported on its own:
        // listing every targeted column would bury the actual problem, which is the value rather than the columns.
        List<?> multiValued = fillNull.multiValuedFill();
        if (multiValued != null) {
            addWarning(
                "Line {}:{}: [FILLNULL] fill value must be a single value, found [{}] values; no columns were filled",
                line,
                column,
                multiValued.size()
            );
            return;
        }

        List<Attribute> unfillable = fillNull.unfillableTargets();
        if (unfillable.isEmpty()) {
            return;
        }
        int shown = Math.min(unfillable.size(), MAX_FIELDS_IN_WARNING);
        StringJoiner names = new StringJoiner(", ");
        for (int i = 0; i < shown; i++) {
            names.add(unfillable.get(i).name());
        }
        String suffix = unfillable.size() > MAX_FIELDS_IN_WARNING
            ? "; only the first " + MAX_FIELDS_IN_WARNING + " of " + unfillable.size() + " fields are shown"
            : "";
        if (fillNull.fillValue() == null) {
            // DEFAULT: the type simply has no default, so the actionable advice is to pass a value.
            addWarning(
                "Line {}:{}: [FILLNULL] the following fields have no default fill value for their type and were left "
                    + "unchanged: [{}]; provide an explicit value{}",
                line,
                column,
                names.toString(),
                suffix
            );
        } else {
            addWarning(
                "Line {}:{}: [FILLNULL] the fill value could not be applied to the following fields, which were left "
                    + "unchanged: [{}]{}",
                line,
                column,
                names.toString(),
                suffix
            );
        }
    }
}
