/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import static org.hamcrest.Matchers.containsString;

/**
 * Systematic grid of unmapped_fields × join-key-mapping cases for LOOKUP JOIN.
 *
 * Axes: mode (nullify / load) × primary-key (mapped / unmapped) × lookup-key (mapped / unmapped).
 * That gives 8 cells. All tests use the same base query shape:
 *   {@code FROM test | LOOKUP JOIN <lookup> ON <key>}
 *
 * "mapped in primary" means the key is present in the employees index mapping
 * (either natively, e.g. {@code emp_no}, or via {@code EVAL language_code = languages}).
 * "unmapped in primary" means the key is absent from the employees mapping
 * (e.g. {@code language_code} without EVAL, or {@code does_not_exist}).
 */
public class AnalyzerUnmapped_JoinKey_Tests extends AnalyzerUnmappedTestBase {

    // -------------------------------------------------------------------------
    // Cat 1: nullify mode
    // -------------------------------------------------------------------------

    public void testNullify_mapped_mapped_succeeds() {
        test().addLanguagesLookup()
            .statement(setUnmappedNullify("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testNullify_mapped_unmapped_rightSideError() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testNullify_unmapped_mapped_succeeds() {
        // nullify inserts NULL for the left key → join succeeds but produces no matches at runtime.
        test().addLanguagesLookup().statement(setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testNullify_unmapped_unmapped_rightSideError() {
        // right side is checked first → errors even though left is also unmapped.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    // -------------------------------------------------------------------------
    // Cat 2: load mode
    // -------------------------------------------------------------------------

    public void testLoad_mapped_mapped_succeeds() {
        test().addLanguagesLookup()
            .statement(setUnmappedLoad("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testLoad_mapped_unmapped_rightSideError() {
        // load() skips LOOKUP EsRelations — right side is never rescued.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testLoad_unmapped_mapped_typeMismatch_error() {
        // load() always promotes unmapped keys to KEYWORD; lookup key is INTEGER → type mismatch.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | LOOKUP JOIN languages_lookup ON language_code"),
                containsString(
                    "JOIN left field [language_code] of type [KEYWORD] is incompatible with right field [language_code] of type [INTEGER]"
                )
            );
    }

    public void testLoad_unmapped_mapped_keywordMatch_succeeds() {
        // load() promotes unmapped key to KEYWORD; lookup key is also KEYWORD → types match.
        test().addLookupIndex("keyword_languages_lookup", keywordLanguagesLookup())
            .statement(setUnmappedLoad("FROM test | LOOKUP JOIN keyword_languages_lookup ON language_code"));
    }

    public void testLoad_unmapped_unmapped_rightSideError() {
        // load() rescues left but skips LOOKUP EsRelations → right still errors.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }
}
