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

    /** Existing mapped values must remain usable under the setting in all ordinary sequence presentations. */
    public void testMappedSequenceFieldsWithNullify() {
        assertValue(primary("", "| EVAL observed = label"), "second");
        assertValue(primary("", "| EVAL observed = MEMBER(1, label)"), "first");
        assertValue(firstPrimary("label"), "first");
    }

    /** A missing ordinary primary demand should acquire NULL without requiring an explicit stage alias. */
    public void testMissingPrimaryFieldNullifies() {
        assertValue(primary("", "| EVAL observed = absent_optional"), null);
    }

    /** A direct missing member demand must retain its member identity when optional field handling runs. */
    public void testMissingMemberFieldNullifies() {
        assertValue(primary("", "| EVAL observed = MEMBER(1, absent_optional)"), null);
    }

    /** A user column matching the diagnostic label cannot satisfy or hide a protected member demand. */
    public void testMissingMemberNullifiesDespiteDiagnosticNameCollision() {
        assertValue(primary("", "| EVAL `MEMBER(1, absent_optional)` = \"ordinary\" | EVAL observed = MEMBER(1, absent_optional)"), null);
    }

    /** Selecting the first primary must route an absent ordinary field to that member. */
    public void testMissingFirstPrimaryFieldNullifies() {
        assertValue(firstPrimary("absent_optional"), null);
    }

    /** An explicit stage expression may resolve the missing value before the sequence presentation is lowered. */
    public void testStageLocalAliasNullifies() {
        assertValue(primary(" | EVAL optional = absent_optional", "| EVAL observed = MEMBER(1, optional)"), null);
    }

    /** Late NULLIFY inputs remain available to stage extraction and MEMBER output. */
    public void testParsingNullifiesMissingInput() {
        for (String extraction : List.of("DISSECT absent_optional \"%{parsed}\"", "GROK absent_optional \"%{WORD:parsed}\"")) {
            assertValue(primary(" | " + extraction, "| EVAL observed = MEMBER(1, parsed)"), null, "keyword");
        }
    }

    /** A generated field removed by an author must not be reintroduced as a nullified source field. */
    public void testNullifyDoesNotRestoreProjectedParsingOutput() {
        for (String extraction : List.of("DISSECT label \"%{parsed}\"", "GROK label \"%{WORD:parsed}\"")) {
            for (String projection : List.of(" | KEEP key", " | DROP parsed")) {
                assertRemoved(primary(" | " + extraction + projection, "| EVAL observed = MEMBER(1, parsed)"), "parsed");
            }
        }
    }

    /** Membership filters preserve the event's optional-field scope. */
    public void testMissingMemberNullifiesThroughStageMembership() {
        for (String predicate : List.of("key IN (FROM " + B + " | KEEP key)", "key NOT IN (FROM " + B + " | KEEP label)")) {
            assertValue(primary(" | WHERE " + predicate, "| EVAL observed = MEMBER(1, absent_optional)"), null);
        }
    }

    /** Nullifying a genuinely absent source field must not resurrect mapped member fields removed by the author. */
    public void testStageKeepAndDropDoNotRestoreRemovedMemberFields() {
        for (String projection : List.of(" | KEEP key", " | DROP label")) {
            assertRemoved(primary(projection, "| EVAL observed = MEMBER(1, label)"), "label");
        }
    }

    /** Ordinary primary fields continue to obey explicit outer projection lifetime under NULLIFY. */
    public void testOuterKeepAndDropDoNotRestoreRemovedPrimaryFields() {
        for (String projection : List.of("KEEP key", "DROP label")) {
            assertRemoved(primary("", "| " + projection + " | EVAL observed = label"), "label");
        }
    }

    private void assertValue(String query, Object expected) {
        assertValue(query, expected, expected == null ? "null" : null);
    }

    private void assertValue(String query, Object expected, String expectedType) {
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            var names = response.columns().stream().map(ColumnInfo::name).toList();
            int column = names.indexOf("observed");
            assertTrue(names.toString(), column >= 0);
            if (expectedType != null) {
                assertEquals(expectedType, response.columns().get(column).outputType());
            }
            var rows = getValuesList(response);
            assertEquals(1, rows.size());
            assertEquals(expected, rows.getFirst().get(column));
        }
    }

    private void assertRemoved(String query, String field) {
        Exception failure = expectThrows(Exception.class, () -> { try (var ignored = run(query)) {} });
        assertThat(failure.getMessage(), containsString("Unknown column"));
        assertThat(failure.getMessage(), containsString(field));
    }

    private static String primary(String firstStageCommands, String outputCommands) {
        return NULLIFY + """
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM %s%s), (FROM %s)
            %s
            | LIMIT 10
            """.formatted(A, firstStageCommands, B, outputCommands);
    }

    private static String firstPrimary(String expression) {
        return NULLIFY + """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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
