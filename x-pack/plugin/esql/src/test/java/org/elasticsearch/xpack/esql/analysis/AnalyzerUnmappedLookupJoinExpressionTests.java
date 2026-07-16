/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.containsString;

/**
 * Analysis-level tests for LOOKUP JOIN ON boolean expression combined with unmapped-fields modes.
 * <p>
 * Primary index: {@code partial_mapping_sample_data} (dynamic:false; mapped:
 * {@code @timestamp}, {@code client_ip}, {@code event_duration:long}, {@code message:keyword}).
 * Lookup index: {@code partial_message_types_lookup} ({@code message:keyword}, {@code message_type:keyword}).
 * <p>
 * Tests that need the verifier but have a field-vs-literal ON condition prepend the cross-side
 * anchor {@code unmapped_event_duration > message_type} to satisfy the parser's requirement for
 * at least one cross-index condition.
 */
public class AnalyzerUnmappedLookupJoinExpressionTests extends AnalyzerUnmappedTestBase {

    private static final Matcher<String> UNSUPPORTED_JOIN_FILTER = containsString("Unsupported join filter expression");

    private static String lookupJoinOn(String onExpr) {
        return "FROM partial_mapping_sample_data | LOOKUP JOIN partial_message_types_lookup ON " + onExpr;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    // ── Same-side left filter (error) ─────────────────────────────────────────

    public void testNullify_leftSide_mappedField_literal_errors() {
        partialMappingTest().statementError(
            setUnmappedNullify(lookupJoinOn("unmapped_event_duration > message_type AND event_duration > 1000000")),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    public void testLoad_leftSide_unmappedField_literal_errors() {
        partialMappingTest().statementError(
            setUnmappedLoad(lookupJoinOn("unmapped_event_duration > message_type AND unmapped_event_duration > \"x\"")),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    // ── Right-side-only filter, KEEP mapped field (succeeds) ──────────────────

    public void testNullify_rightSide_literal_keepMapped_doesNotThrow() {
        assumeTrue(
            "requires LOOKUP JOIN with full-text function support",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX.isEnabled()
        );
        partialMappingTest().minimumTransportVersion(Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
            .statement(
                setUnmappedNullify(
                    lookupJoinOn("unmapped_event_duration > message_type AND message_type > \"error\"")
                        + " | KEEP @timestamp, event_duration, message_type"
                )
            );
    }

    public void testNullify_rightSide_selfComp_keepMapped_errors() {
        partialMappingTest().statementError(
            setUnmappedNullify(
                lookupJoinOn("unmapped_event_duration > message_type AND message_type > message_type")
                    + " | KEEP @timestamp, event_duration, message_type"
            ),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    public void testLoad_rightSide_literal_keepMapped_doesNotThrow() {
        assumeTrue(
            "requires LOOKUP JOIN with full-text function support",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX.isEnabled()
        );
        partialMappingTest().minimumTransportVersion(Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
            .statement(
                setUnmappedLoad(
                    lookupJoinOn("unmapped_event_duration > message_type AND message_type > \"error\"")
                        + " | KEEP @timestamp, event_duration, message_type"
                )
            );
    }

    public void testLoad_rightSide_selfComp_keepMapped_errors() {
        partialMappingTest().statementError(
            setUnmappedLoad(
                lookupJoinOn("unmapped_event_duration > message_type AND message_type > message_type")
                    + " | KEEP @timestamp, event_duration, message_type"
            ),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    // ── Right-side-only filter, KEEP unmapped field (succeeds / errors) ───────

    public void testNullify_rightSide_literal_keepUnmapped_doesNotThrow() {
        assumeTrue(
            "requires LOOKUP JOIN with full-text function support",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX.isEnabled()
        );
        partialMappingTest().minimumTransportVersion(Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
            .statement(
                setUnmappedNullify(
                    lookupJoinOn("unmapped_event_duration > message_type AND message_type > \"error\"")
                        + " | EVAL unmapped = unmapped_event_duration"
                        + " | KEEP @timestamp, unmapped, message_type"
                )
            );
    }

    public void testNullify_rightSide_selfComp_keepUnmapped_errors() {
        partialMappingTest().statementError(
            setUnmappedNullify(
                lookupJoinOn("unmapped_event_duration > message_type AND message_type > message_type")
                    + " | EVAL unmapped = unmapped_event_duration"
                    + " | KEEP @timestamp, unmapped, message_type"
            ),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    public void testLoad_rightSide_literal_keepUnmapped_doesNotThrow() {
        assumeTrue(
            "requires LOOKUP JOIN with full-text function support",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX.isEnabled()
        );
        partialMappingTest().minimumTransportVersion(Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
            .statement(
                setUnmappedLoad(
                    lookupJoinOn("unmapped_event_duration > message_type AND message_type > \"error\"")
                        + " | EVAL unmapped = unmapped_event_duration"
                        + " | KEEP @timestamp, unmapped, message_type"
                )
            );
    }

    public void testLoad_rightSide_selfComp_keepUnmapped_errors() {
        partialMappingTest().statementError(
            setUnmappedLoad(
                lookupJoinOn("unmapped_event_duration > message_type AND message_type > message_type")
                    + " | EVAL unmapped = unmapped_event_duration"
                    + " | KEEP @timestamp, unmapped, message_type"
            ),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    // ── Cross-side comparison ─────────────────────────────────────────────────

    public void testNullify_crossSide_longVsKeyword_typeMismatch_errors() {
        partialMappingTest().statementError(
            setUnmappedNullify(lookupJoinOn("event_duration > message_type")),
            containsString("message_type")
        );
    }

    // nullify makes the unmapped field null-typed; null vs keyword is type-compatible, so analyzer accepts.
    public void testNullify_crossSide_unmappedField_doesNotThrow() {
        partialMappingTest().statement(
            setUnmappedNullify(
                lookupJoinOn("unmapped_event_duration > message_type") + " | KEEP @timestamp, unmapped_event_duration, message_type"
            )
        );
    }

    public void testLoad_crossSide_longVsKeyword_typeMismatch_errors() {
        partialMappingTest().statementError(setUnmappedLoad(lookupJoinOn("event_duration > message_type")), containsString("message_type"));
    }

    // load promotes the unmapped field to PotentiallyUnmappedKeyword (KEYWORD), which is type-compatible with the lookup's KEYWORD.
    public void testLoad_crossSide_unmappedPuk_keywordVsKeyword_doesNotThrow() {
        partialMappingTest().statement(
            setUnmappedLoad(
                lookupJoinOn("unmapped_event_duration > message_type") + " | KEEP @timestamp, unmapped_event_duration, message_type"
            )
        );
    }

    // ── Nonexistent field in ON expression (error) ────────────────────────────

    public void testNullify_nonexistentField_errors() {
        partialMappingTest().statementError(
            setUnmappedNullify(lookupJoinOn("unmapped_event_duration > message_type AND nonexistent > \"a\"")),
            UNSUPPORTED_JOIN_FILTER
        );
    }

    public void testLoad_nonexistentField_errors() {
        partialMappingTest().statementError(
            setUnmappedLoad(lookupJoinOn("unmapped_event_duration > message_type AND nonexistent > \"a\"")),
            UNSUPPORTED_JOIN_FILTER
        );
    }
}
