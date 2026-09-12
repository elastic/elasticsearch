/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ForkTests extends ESTestCase {

    private static ReferenceAttribute text(String name, String valuesAnalyzer) {
        return new ReferenceAttribute(EMPTY, null, name, DataType.TEXT, Nullability.TRUE, null, false, valuesAnalyzer);
    }

    public void testNoConflictWhenTypeAndAnalyzerAgree() {
        assertThat(Fork.checkForMergeConflict(text("t", "whitespace"), text("t", "whitespace")), nullValue());
        assertThat(Fork.checkForMergeConflict(text("t", null), text("t", null)), nullValue());
    }

    /**
     * Declaring the default explicitly has to compare equal to declaring nothing, since they name the same analyzer.
     */
    public void testExplicitStandardAgreesWithNoDeclaration() {
        assertThat(Fork.checkForMergeConflict(text("t", "standard"), text("t", null)), nullValue());
        assertThat(Fork.checkForMergeConflict(text("t", null), text("t", "standard")), nullValue());
    }

    /**
     * Declaring nothing means the standard analyzer, so it conflicts with a sibling naming a different one. The
     * caller exempts a column that has no values to analyze; the comparison itself does not weaken for everyone.
     */
    public void testUndeclaredConflictsWithADeclaredAnalyzer() {
        assertThat(
            Fork.checkForMergeConflict(text("t", null), text("t", "whitespace")),
            equalTo(new Fork.MergeConflict("values analyzers", "standard", "whitespace"))
        );
        assertThat(
            Fork.checkForMergeConflict(text("t", "whitespace"), text("t", null)),
            equalTo(new Fork.MergeConflict("values analyzers", "whitespace", "standard"))
        );
    }

    public void testConflictingValuesAnalyzers() {
        var conflict = Fork.checkForMergeConflict(text("t", "english"), text("t", "whitespace"));
        assertThat(conflict, equalTo(new Fork.MergeConflict("values analyzers", "english", "whitespace")));
    }

    /**
     * A field carries no declaration of its own, so it reads as the standard analyzer. This is why an indexed text
     * field surfacing through a branch merge is searched with the standard analyzer rather than its mapping's.
     */
    public void testFieldConflictsWithADeclaredAnalyzer() {
        assertThat(
            Fork.checkForMergeConflict(fieldAttribute("t", DataType.TEXT), text("t", "whitespace")),
            equalTo(new Fork.MergeConflict("values analyzers", "standard", "whitespace"))
        );
    }

    public void testConflictingDataTypes() {
        var conflict = Fork.checkForMergeConflict(fieldAttribute("t", DataType.INTEGER), fieldAttribute("t", DataType.KEYWORD));
        assertThat(conflict, equalTo(new Fork.MergeConflict("data types", "INTEGER", "KEYWORD")));
    }

    /**
     * Reported before the analyzer, so a column disagreeing on both gets the message naming the cause a reader can
     * act on rather than whichever check happened to run first.
     */
    public void testDataTypeConflictTakesPrecedenceOverAnalyzer() {
        var conflict = Fork.checkForMergeConflict(
            new ReferenceAttribute(EMPTY, null, "t", DataType.KEYWORD, Nullability.TRUE, null, false, "english"),
            text("t", "whitespace")
        );
        assertThat(conflict, equalTo(new Fork.MergeConflict("data types", "KEYWORD", "TEXT")));
    }
}
