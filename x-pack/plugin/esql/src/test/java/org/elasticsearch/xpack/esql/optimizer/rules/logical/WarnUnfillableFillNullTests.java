/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;

/**
 * FILLNULL never fails over a column it cannot fill; it leaves the column alone and warns. These pin the exact warnings,
 * which is the only thing the user sees, and that the shape does not depend on how the columns were selected: one capped
 * summary, always.
 */
public class WarnUnfillableFillNullTests extends ESTestCase {

    private static FillNull fillNull(
        @Nullable Expression fillValue,
        List<Attribute> output,
        List<NamedExpression> targets,
        boolean allColumns
    ) {
        LocalRelation child = new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
        return new FillNull(Source.EMPTY, child, fillValue, targets, allColumns).withConfiguration(TEST_CFG);
    }

    private static void applyRule(FillNull fillNull) {
        new WarnUnfillableFillNull().apply(fillNull);
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static Literal integer(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    /** The same column, unfillable for the same reason, warns identically whether it was named or swept up by `*`. */
    public void testWarningDoesNotDependOnHowTheColumnWasSelected() {
        Attribute k = getFieldAttribute("first_name", DataType.KEYWORD);
        List<Attribute> output = List.of(k);
        String expected = "Line -1:-1: [FILLNULL] the fill value could not be applied to the following fields, "
            + "which were left unchanged: [first_name]";

        applyRule(fillNull(integer(0), output, List.of(k), false));
        assertWarnings(expected);

        applyRule(fillNull(integer(0), output, List.of(), true));
        assertWarnings(expected);

        applyRule(fillNull(integer(0), output, List.of(k), true));
        assertWarnings(expected);
    }

    /** Several unfillable columns are one warning, not one each - whatever the target form. */
    public void testSingleSummaryWarningForNamedTargets() {
        Attribute a = getFieldAttribute("first_name", DataType.KEYWORD);
        Attribute b = getFieldAttribute("gender", DataType.KEYWORD);
        applyRule(fillNull(integer(0), List.of(a, b), List.of(a, b), false));
        assertWarnings(
            "Line -1:-1: [FILLNULL] the fill value could not be applied to the following fields, "
                + "which were left unchanged: [first_name, gender]"
        );
    }

    public void testDefaultWarningNamesTheTypeDefaultProblem() {
        Attribute d = getFieldAttribute("birth_date", DataType.DATETIME);
        applyRule(fillNull(null, List.of(d), List.of(d), false));
        assertWarnings(
            "Line -1:-1: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [birth_date]; provide an explicit value"
        );
    }

    /** A null-typed column can never be filled, and used to be skipped without telling anyone. */
    public void testNullTypedColumnIsReported() {
        Attribute n = getFieldAttribute("a", DataType.NULL);
        applyRule(fillNull(keyword("x"), List.of(n), List.of(n), false));
        assertWarnings(
            "Line -1:-1: [FILLNULL] the fill value could not be applied to the following fields, " + "which were left unchanged: [a]"
        );
    }

    public void testMoreThanTenUnfillableColumnsAreCapped() {
        List<Attribute> output = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            output.add(getFieldAttribute("d" + i, DataType.DATETIME));
        }
        applyRule(fillNull(null, output, List.of(), true));
        assertWarnings(
            "Line -1:-1: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d0, d1, d2, d3, d4, d5, d6, d7, d8, d9]; provide an explicit value; "
                + "only the first 10 of 12 fields are shown"
        );
    }

    /** A multi-valued value fills nothing, so the value is the problem to report - not every column in the list. */
    public void testMultiValuedFillValueIsReportedOnItsOwn() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Literal multi = new Literal(Source.EMPTY, List.of(1, 2), DataType.INTEGER);
        applyRule(fillNull(multi, List.of(i), List.of(i), false));
        assertWarnings("Line -1:-1: [FILLNULL] fill value must be a single value, found [2] values; no columns were filled");
    }

    /** An explicit NULL means "do not fill", so nothing was unexpectedly left unchanged. */
    public void testExplicitNullFillIsSilent() {
        Attribute d = getFieldAttribute("birth_date", DataType.DATETIME);
        applyRule(fillNull(new Literal(Source.EMPTY, null, DataType.NULL), List.of(d), List.of(d), false));
        ensureNoWarnings();
    }

    public void testNothingUnfillableIsSilent() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        applyRule(fillNull(integer(0), List.of(i), List.of(i), false));
        ensureNoWarnings();
    }
}
