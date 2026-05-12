/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.is;

/**
 * Non-key output field semantics for LOOKUP JOIN.
 *
 * The join key ({@code language_code}) is always mapped on both sides via EVAL, so the join
 * itself succeeds in all cases. The varying axis is: does the *non-key field* exist in the
 * primary index, the lookup index, both, or neither?
 *
 * Query shape: {@code FROM test | EVAL language_code = languages | LOOKUP JOIN custom_lookup ON language_code}
 *
 * custom_lookup fields:
 *   - {@code salary} (KEYWORD) — same name as primary's INTEGER salary; lookup type must win
 *   - {@code lookup_only} (KEYWORD) — present only in the lookup
 *   - (primary has {@code first_name} (KEYWORD) which custom_lookup does not)
 */
public class AnalyzerUnmapped_NonKeyField_Tests extends AnalyzerUnmappedTestBase {

    private static final String BASE_QUERY = "FROM test | EVAL language_code = languages | LOOKUP JOIN custom_lookup ON language_code";

    // -------------------------------------------------------------------------
    // nullify mode (default): field presence determines output composition
    // -------------------------------------------------------------------------

    public void testNullify_inBoth_lookupTypeWins() {
        // salary: INT in primary, KEYWORD in lookup — lookup's type and value override the primary's.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY);
        var salary = plan.output().stream().filter(a -> "salary".equals(a.name())).findFirst().orElseThrow();
        assertThat(salary.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inPrimaryOnly_passesThrough() {
        // first_name: present in primary (KEYWORD), absent from custom_lookup — passes through unchanged.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY);
        var firstName = plan.output().stream().filter(a -> "first_name".equals(a.name())).findFirst().orElseThrow();
        assertThat(firstName.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inLookupOnly_appended() {
        // lookup_only: absent from primary, present in lookup (KEYWORD) — appended to the join output.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY);
        var lookupOnly = plan.output().stream().filter(a -> "lookup_only".equals(a.name())).findFirst().orElseThrow();
        assertThat(lookupOnly.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inNeither_absentFromOutput() {
        // totally_absent: not in primary, not in lookup — must not appear in the join output.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY);
        assertFalse(plan.output().stream().anyMatch(a -> "totally_absent".equals(a.name())));
    }

    // -------------------------------------------------------------------------
    // load mode: field absent from both indices is loaded from primary _source
    // -------------------------------------------------------------------------

    public void testLoad_inNeither_loadedAsKeyword() {
        // does_not_exist: absent from primary and lookup. load() adds it to the primary EsRelation
        // as a PotentiallyUnmappedKeywordEsField; it flows through the join and resolves as KEYWORD.
        var plan = test().addLanguagesLookup()
            .statement(
                setUnmappedLoad(
                    "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | EVAL x = does_not_exist"
                )
            );
        var x = plan.output().stream().filter(a -> "x".equals(a.name())).findFirst().orElseThrow();
        assertThat(x.dataType(), is(DataType.KEYWORD));
    }
}
