/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;

/** Optional field handling must cross compiler-generated projections without restoring user-removed fields. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceUnmappedFieldsIT extends AbstractSequenceIntegTestCase {
    private static final String A = "sequence-unmapped-a";
    private static final String B = "sequence-unmapped-b";
    private static final String NULLIFY = "SET unmapped_fields = \"nullify\";\n";

    /** Both explicit mappings omit the optional field; a matching pair remains available in every probe. */
    @Before
    public void seed() {
        for (String index : List.of(A, B)) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0))
                    .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "label", "type=keyword")
            );
        }
        ensureGreen(A, B);
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex(A).setId("A").setSource(document(true)))
                .add(prepareIndex(B).setId("B").setSource(document(false)))
                .get()
                .hasFailures()
        );
        refresh(A, B);
    }

    /** The ordinary source path establishes the requested NULLIFY semantics independently of SEQUENCE. */
    public void testOrdinaryNullifyReturnsNullForMissingField() {
        assertValue(NULLIFY + "FROM " + B + " | EVAL observed = absent_optional | LIMIT 10", null, false);
    }

    /** Existing mapped values must remain usable under the setting in all ordinary sequence presentations. */
    public void testMappedSequenceFieldsWithNullifyWithoutRetention() {
        assertValue(primary("", "| EVAL observed = label", false), "second", false);
        assertValue(primary("", "| EVAL observed = MEMBER(1, label)", false), "first", false);
        assertValue(wide("stage1.label"), "first", false);
    }

    /** Retention is tested separately so a capture-specific failure cannot hide the ordinary controls. */
    public void testMappedSequenceFieldsWithNullifyAndRetention() {
        assertValue(primary("", "| EVAL observed = label", true), "second", true);
        assertValue(primary("", "| EVAL observed = MEMBER(1, label)", true), "first", true);
    }

    /** A missing ordinary primary demand should acquire NULL without requiring an explicit stage alias. */
    public void testMissingPrimaryFieldNullifiesWithoutRetention() {
        assertValue(primary("", "| EVAL observed = absent_optional", false), null, false);
    }

    /** Adding an optional ordinary output must preserve both retained originals and their match association. */
    public void testMissingPrimaryFieldNullifiesWithRetention() {
        assertValue(primary("", "| EVAL observed = absent_optional", true), null, true);
    }

    /** A direct missing member demand must retain its member identity when optional field handling runs. */
    public void testMissingMemberFieldNullifiesWithoutRetention() {
        assertValue(primary("", "| EVAL observed = MEMBER(1, absent_optional)", false), null, false);
    }

    /** A null-valued member output must not turn authentic original bindings into null-valued substitutes. */
    public void testMissingMemberFieldNullifiesWithRetention() {
        assertValue(primary("", "| EVAL observed = MEMBER(1, absent_optional)", true), null, true);
    }

    /** A user column matching the diagnostic label cannot satisfy or hide a protected member demand. */
    public void testMissingMemberNullifiesDespiteDiagnosticNameCollisionWithoutRetention() {
        assertValue(
            primary("", "| EVAL `MEMBER(1, absent_optional)` = \"ordinary\" | EVAL observed = MEMBER(1, absent_optional)", false),
            null,
            false
        );
    }

    /** Retained originals do not make the diagnostic label a valid substitute for the absent member field. */
    public void testMissingMemberNullifiesDespiteDiagnosticNameCollisionWithRetention() {
        assertValue(
            primary("", "| EVAL `MEMBER(1, absent_optional)` = \"ordinary\" | EVAL observed = MEMBER(1, absent_optional)", true),
            null,
            true
        );
    }

    /** Legacy stage qualification must refer to a stage field rather than synthesize a twice-qualified source name. */
    public void testMissingStageQualifiedFieldNullifies() {
        assertValue(wide("stage1.absent_optional"), null, false);
    }

    /** An explicit stage expression may resolve the missing value before the sequence presentation is lowered. */
    public void testStageLocalAliasNullifiesWithoutRetention() {
        assertValue(primary(" | EVAL optional = absent_optional", "| EVAL observed = MEMBER(1, optional)", false), null, false);
    }

    /** Retained-source capture must not hide a late null-filled input needed by a stage expression. */
    public void testStageLocalAliasNullifiesWithRetention() {
        assertValue(primary(" | EVAL optional = absent_optional", "| EVAL observed = MEMBER(1, optional)", true), null, true);
    }

    /** Membership filters preserve the event's optional-field scope and the retained source association. */
    public void testMissingMemberNullifiesThroughStageMembership() {
        for (boolean retained : List.of(false, true)) {
            for (String predicate : List.of("key IN (FROM " + B + " | KEEP key)", "key NOT IN (FROM " + B + " | KEEP label)")) {
                assertValue(primary(" | WHERE " + predicate, "| EVAL observed = MEMBER(1, absent_optional)", retained), null, retained);
            }
        }
    }

    /** Nullifying a genuinely absent source field must not resurrect mapped member fields removed by the author. */
    public void testStageKeepAndDropDoNotRestoreRemovedMemberFields() {
        for (boolean retained : List.of(false, true)) {
            for (String projection : List.of(" | KEEP key", " | DROP label")) {
                assertRemoved(primary(projection, "| EVAL observed = MEMBER(1, label)", retained), "label");
            }
        }
    }

    /** Ordinary primary fields continue to obey explicit outer projection lifetime under NULLIFY. */
    public void testOuterKeepAndDropDoNotRestoreRemovedPrimaryFields() {
        for (boolean retained : List.of(false, true)) {
            for (String projection : List.of("KEEP key", "DROP label")) {
                assertRemoved(primary("", "| " + projection + " | EVAL observed = label", retained), "label");
            }
        }
    }

    private void assertValue(String query, Object expected, boolean retained) {
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            var names = response.columns().stream().map(ColumnInfo::name).toList();
            int column = names.indexOf("observed");
            assertTrue(names.toString(), column >= 0);
            if (expected == null) {
                assertEquals("null", response.columns().get(column).outputType());
            }
            var rows = getValuesList(response);
            assertEquals(1, rows.size());
            assertEquals(expected, rows.getFirst().get(column));
            if (retained) {
                var description = SequenceMatchDescription.fromColumns(response.columns());
                assertEquals(2, description.primaryStage());
                var decoded = description.decodeRow(rows.getFirst(), SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT);
                assertEquals(List.of("shared"), decoded.keys());
                assertEquals(2, decoded.members().size());
                assertEquals(List.of(A, B), decoded.members().stream().map(SequenceMatchDescription.Member::index).toList());
                assertEquals(List.of("A", "B"), decoded.members().stream().map(SequenceMatchDescription.Member::id).toList());
                assertEquals(document(true), decoded.members().getFirst().source());
                assertEquals(document(false), decoded.members().getLast().source());
            }
        }
    }

    private void assertRemoved(String query, String field) {
        Exception failure = expectThrows(Exception.class, () -> { try (var ignored = run(query)) {} });
        assertThat(failure.getMessage(), containsString("Unknown column"));
        assertThat(failure.getMessage(), containsString(field));
    }

    private static String primary(String firstStageCommands, String outputCommands, boolean retained) {
        return NULLIFY + """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 2
            (FROM %s%s), (FROM %s)
            %s
            | LIMIT 10%s
            """.formatted(A, firstStageCommands, B, outputCommands, retained ? " | RETURN MATCHES" : "");
    }

    private static String wide(String expression) {
        return NULLIFY + """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM %s), (FROM %s)
            | EVAL observed = %s | LIMIT 10
            """.formatted(A, B, expression);
    }

    private static Map<String, Object> document(boolean first) {
        return Map.of(
            "@timestamp",
            first ? "2026-11-10T12:00:00Z" : "2026-11-10T12:00:01Z",
            "key",
            "shared",
            "label",
            first ? "first" : "second"
        );
    }
}
