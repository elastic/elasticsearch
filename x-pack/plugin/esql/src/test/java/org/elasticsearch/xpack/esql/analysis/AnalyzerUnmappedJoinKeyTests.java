/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Systematic grid of unmapped_fields × join-key-mapping cases for LOOKUP JOIN.
 *
 * Axes: mode (nullify / load) × primary-key (mapped / unmapped) × lookup-key (mapped / unmapped).
 * That gives 8 cells.
 *
 * "mapped in primary" means the key is present in the employees index mapping
 * (either natively, e.g. {@code emp_no}, or via {@code EVAL language_code = languages}).
 * "unmapped in primary" means the key is absent from the employees mapping
 * (e.g. {@code language_code} without EVAL, or {@code does_not_exist}).
 */
public class AnalyzerUnmappedJoinKeyTests extends AnalyzerUnmappedTestBase {

    public AnalyzerUnmappedJoinKeyTests(VersionMode versionMode) {
        super(versionMode);
    }

    private static String lookupJoinQuery(String lookup, String key) {
        return "FROM test | LOOKUP JOIN " + lookup + " ON " + key;
    }

    // -------------------------------------------------------------------------
    // Category 1: nullify mode
    // -------------------------------------------------------------------------

    public void testNullify_mapped_mapped_doesNotThrow() {
        test().addLanguagesLookup()
            .statement(setUnmappedNullify("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testNullify_mapped_unmapped_rightSideError() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify(lookupJoinQuery("languages_lookup", "emp_no")),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testNullify_unmapped_mapped_doesNotThrow() {
        // nullify inserts NULL for the left key → join succeeds but produces no matches at runtime.
        test().addLanguagesLookup().statement(setUnmappedNullify(lookupJoinQuery("languages_lookup", "language_code")));
    }

    public void testNullify_unmapped_unmapped_rightSideError() {
        // right side is checked first → errors even though left is also unmapped.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify(lookupJoinQuery("languages_lookup", "does_not_exist")),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    // -------------------------------------------------------------------------
    // Category 2: load mode
    // -------------------------------------------------------------------------

    public void testLoad_mapped_mapped_doesNotThrow() {
        test().addLanguagesLookup()
            .statement(setUnmappedLoad("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testLoad_mapped_unmapped_rightSideError() {
        // load() skips LOOKUP EsRelations — right side is never rescued.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad(lookupJoinQuery("languages_lookup", "emp_no")),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testLoad_unmapped_mapped_typeMismatch_error() {
        // load() always promotes unmapped keys to KEYWORD; lookup key is INTEGER → type mismatch.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad(lookupJoinQuery("languages_lookup", "language_code")),
                containsString(
                    "JOIN left field [language_code] of type [KEYWORD] is incompatible with right field [language_code] of type [INTEGER]"
                )
            );
    }

    public void testLoad_unmapped_mapped_keywordMatch_doesNotThrow() {
        // load() promotes unmapped key to KEYWORD; lookup key is also KEYWORD → types match.
        test().addLookupIndex("keyword_languages_lookup", keywordLanguagesLookup())
            .statement(setUnmappedLoad(lookupJoinQuery("keyword_languages_lookup", "language_code")));
    }

    public void testLoad_unmapped_unmapped_rightSideError() {
        // load() rescues left but skips LOOKUP EsRelations → right still errors.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad(lookupJoinQuery("languages_lookup", "does_not_exist")),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    // -------------------------------------------------------------------------
    // Category 5: a join ON expression with an unmapped left key
    // -------------------------------------------------------------------------

    private static final String UNMAPPED_LEFT_KEY =
        "FROM test | RENAME languages AS lang_left, does_not_exist AS name_left | LOOKUP JOIN languages_lookup ON ";

    /**
     * The parser puts both operands of every equality of a join ON expression in the left fields, as it cannot tell the sides apart. With
     * an unmapped left key the join does not resolve before the unmapped fields do, and the right operand was loaded onto the left side
     * as an unmapped field, which made the name ambiguous once the join resolved.
     */
    public void testExpressionJoinWithUnmappedLeftKeyDoesNotLoadTheRightOperand() {
        for (String on : new String[] {
            "name_left == language_name",
            "lang_left == language_code AND name_left == language_name",
            "lang_left == language_code AND name_left == language_name AND lang_left < language_code" }) {
            for (String setting : new String[] {
                setUnmappedNullify(UNMAPPED_LEFT_KEY + on),
                setUnmappedLoad(UNMAPPED_LEFT_KEY + on),
                setUnmappedLoadAll(UNMAPPED_LEFT_KEY + on) }) {
                LogicalPlan plan = test().addLanguagesLookup().statement(setting);
                for (EsRelation relation : plan.collect(EsRelation.class)) {
                    if (relation.indexMode() != IndexMode.LOOKUP) {
                        assertThat(setting, Expressions.names(relation.output()), not(hasItem("language_name")));
                        assertThat(setting, Expressions.names(relation.output()), not(hasItem("language_code")));
                    }
                }
            }
        }
    }
}
