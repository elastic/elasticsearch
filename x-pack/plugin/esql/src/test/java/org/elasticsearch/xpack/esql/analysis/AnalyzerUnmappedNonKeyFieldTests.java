/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
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
public class AnalyzerUnmappedNonKeyFieldTests extends AnalyzerUnmappedTestBase {

    private static final String BASE_QUERY = "FROM test | EVAL language_code = languages | LOOKUP JOIN custom_lookup ON language_code";

    // -------------------------------------------------------------------------
    // nullify mode (default): field presence determines output composition
    // -------------------------------------------------------------------------

    public void testNullify_inBoth_lookupTypeWins() {
        // salary: INT in primary, KEYWORD in lookup — lookup's type and value override the primary's.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY + " | KEEP salary");
        var salary = singleValue(plan.output().stream().filter(a -> "salary".equals(a.name())).toList());
        assertThat(salary.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inPrimaryOnly_passesThrough() {
        // first_name: present in primary (KEYWORD), absent from custom_lookup — passes through unchanged.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY + " | KEEP first_name");
        var firstName = singleValue(plan.output().stream().filter(a -> "first_name".equals(a.name())).toList());
        assertThat(firstName.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inLookupOnly_appended() {
        // lookup_only: absent from primary, present in lookup (KEYWORD) — appended to the join output.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY + " | KEEP lookup_only");
        var lookupOnly = singleValue(plan.output().stream().filter(a -> "lookup_only".equals(a.name())).toList());
        assertThat(lookupOnly.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_inNeither_absentFromOutput() {
        // No KEEP: nullify() can't inject NULL above the join boundary — KEEP would error.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields()).statement(BASE_QUERY);
        assertFalse(plan.output().stream().anyMatch(a -> "totally_absent".equals(a.name())));
    }

    public void testNullify_inNeither_keepErrors() {
        // nullify() can't inject NULL above the join boundary.
        test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields())
            .statementError(BASE_QUERY + " | KEEP totally_absent", containsString("Unknown column [totally_absent]"));
    }

    // -------------------------------------------------------------------------
    // load mode: same field-presence combinations as above
    // -------------------------------------------------------------------------

    public void testLoad_inBoth_lookupTypeWins() {
        // salary: INT in primary, KEYWORD in lookup — lookup type wins regardless of mode.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields())
            .statement(setUnmappedLoad(BASE_QUERY + " | KEEP salary"));
        var salary = singleValue(plan.output().stream().filter(a -> "salary".equals(a.name())).toList());
        assertThat(salary.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_inPrimaryOnly_passesThrough() {
        // first_name: present in primary, absent from lookup — passes through regardless of mode.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields())
            .statement(setUnmappedLoad(BASE_QUERY + " | KEEP first_name"));
        var firstName = singleValue(plan.output().stream().filter(a -> "first_name".equals(a.name())).toList());
        assertThat(firstName.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_inLookupOnly_appended() {
        // lookup_only: absent from primary, present in lookup — appended regardless of mode.
        var plan = test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields())
            .statement(setUnmappedLoad(BASE_QUERY + " | KEEP lookup_only"));
        var lookupOnly = singleValue(plan.output().stream().filter(a -> "lookup_only".equals(a.name())).toList());
        assertThat(lookupOnly.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_inNeither_loadedAsKeyword() {
        // absent from all indexes — load() injects PotentiallyUnmappedKeyword into primary; flows through join as KEYWORD.
        var plan = test().addLanguagesLookup()
            .statement(
                setUnmappedLoad(
                    "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | KEEP does_not_exist"
                )
            );
        var field = singleValue(plan.output().stream().filter(a -> "does_not_exist".equals(a.name())).toList());
        assertThat(field.dataType(), is(DataType.KEYWORD));
        assertThat(field, instanceOf(FieldAttribute.class));
        assertThat(((FieldAttribute) field).field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }
}
