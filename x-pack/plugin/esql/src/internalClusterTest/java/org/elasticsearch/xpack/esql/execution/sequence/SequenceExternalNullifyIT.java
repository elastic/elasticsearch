/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Finding 1 compares each MEMBER lifetime with the same ordinary reference under NULLIFY. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExternalNullifyIT extends AbstractSequenceIntegTestCase {
    private static final String A = "sequence-external-nullify-a";
    private static final String B = "sequence-external-nullify-b";
    private static final String NULLIFY = "SET unmapped_fields=\"nullify\";\n";

    /** The earlier and own stages deliberately have different non-null values under the same field name. */
    @Before
    public void createEvents() {
        for (String index : List.of(A, B)) {
            Map<String, Object> fields = new HashMap<>(
                Map.of("@timestamp", Map.of("type", "date_nanos"), "key", Map.of("type", "keyword"), "label", Map.of("type", "keyword"))
            );
            if (index.equals(B)) {
                fields.put("later_only", Map.of("type", "keyword"));
            }
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(Map.of("properties", fields)));
        }
        ensureGreen(A, B);
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex(A).setId("A").setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "label", "left"))
                .add(
                    prepareIndex(B).setId("B")
                        .setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "label", "right", "later_only", "own-stage")
                )
                .get()
                .hasFailures()
        );
        refresh(A, B);
    }

    /** A genuinely unmapped earlier value follows plain NULLIFY, rather than borrowing an own-stage value. */
    public void testUnmappedMemberAfterCommandMatchesPlainReference() {
        assertAfter("", "absent");
    }

    /** A user KEEP is a lifetime boundary, even when missing source fields may nullify. */
    public void testKeptAwayMemberAfterCommandMatchesPlainReference() {
        assertAfter(" | KEEP key", "label");
    }

    /** DROP cannot be undone by losing the MEMBER node's identity. */
    public void testDroppedMemberAfterCommandMatchesPlainReference() {
        assertAfter(" | DROP label", "label");
    }

    /** The old name after RENAME has exactly the same validity as an ordinary old-name reference. */
    public void testRenamedAwayMemberAfterCommandMatchesPlainReference() {
        assertAfter(" | RENAME label AS renamed", "label");
    }

    /** The condition must preserve genuine absence without capturing the later event's schema. */
    public void testUnmappedMemberConditionMatchesPlainReference() {
        assertCondition("", "absent", " IS NULL");
    }

    /** A missing earlier-stage KEEP output must not silently turn into an admissible local NULL condition. */
    public void testKeptAwayMemberConditionMatchesPlainReference() {
        assertCondition(" | KEEP key", "label", " IS NULL");
    }

    /** A DROP boundary must survive custom diagnostic rewriting in condition resolution. */
    public void testDroppedMemberConditionMatchesPlainReference() {
        assertCondition(" | DROP label", "label", " IS NULL");
    }

    /** A RENAME boundary must survive custom diagnostic rewriting in condition resolution. */
    public void testRenamedAwayMemberConditionMatchesPlainReference() {
        assertCondition(" | RENAME label AS renamed", "label", " IS NULL");
    }

    /** Valid renamed output remains usable in both positions and still selects the earlier value. */
    public void testRenamedMemberPositiveControls() {
        assertAfter(" | RENAME label AS renamed", "renamed");
        assertCondition(" | RENAME label AS renamed", "renamed", " == \"left\"");
        Probe ordinary = probe(NULLIFY + "FROM " + A + " | RENAME label AS renamed | EVAL observed=renamed | KEEP observed | LIMIT 10");
        assertFalse(ordinary.verificationFailure());
        assertEquals(List.of(List.of("left")), ordinary.rows());
    }

    /** A field that exists only later must nullify in the selected earlier member, without borrowing the later value. */
    public void testUnmappedMemberDoesNotBindLaterStageValue() {
        String sequence = NULLIFY + "SEQUENCE BY key WITHIN 10s PRIMARY 2 (FROM " + A + "),(FROM " + B + ")";
        Probe result = probe(sequence + " | EVAL observed=MEMBER(1,later_only) | KEEP observed,later_only | LIMIT 10");
        assertFalse(result.verificationFailure());
        assertEquals(List.of(Arrays.asList(null, "own-stage")), result.rows());
        assertCondition("", "later_only", " IS NULL");
        for (String condition : List.of(" IS NOT NULL", " == \"own-stage\"")) {
            Probe rejected = probe(
                NULLIFY
                    + "SEQUENCE BY key WITHIN 10s PRIMARY 2 (FROM "
                    + A
                    + "),(FROM "
                    + B
                    + " | WHERE MEMBER(1,later_only)"
                    + condition
                    + ") | KEEP key | LIMIT 10"
            );
            assertFalse(rejected.verificationFailure());
            assertEquals(List.of(), rejected.rows());
        }
    }

    /** The successful value control asserts both stages explicitly instead of only comparing two query results. */
    public void testRenamedMemberAndPrimaryValuesStayDistinct() {
        Probe result = probe(
            NULLIFY
                + "SEQUENCE BY key WITHIN 10s PRIMARY 2 (FROM "
                + A
                + " | RENAME label AS renamed),(FROM "
                + B
                + " | WHERE MEMBER(1,renamed)==\"left\" AND label==\"right\")"
                + " | EVAL observed=MEMBER(1,renamed) | KEEP observed,label | LIMIT 10"
        );
        assertFalse(result.verificationFailure());
        assertEquals(List.of(List.of("left", "right")), result.rows());
    }

    private void assertAfter(String commands, String field) {
        Probe ordinary = probe(NULLIFY + "FROM " + A + commands + " | EVAL observed=" + field + " | KEEP observed | LIMIT 10");
        Probe sequence = probe(
            NULLIFY
                + "SEQUENCE BY key WITHIN 10s PRIMARY 2 (FROM "
                + A
                + commands
                + "),(FROM "
                + B
                + ")"
                + " | EVAL observed=MEMBER(1,"
                + field
                + ") | KEEP observed | LIMIT 10"
        );
        assertParity(field, ordinary, sequence);
        if (ordinary.verificationFailure() == false) {
            assertEquals(1, ordinary.rows().size());
        }
    }

    private void assertCondition(String commands, String field, String suffix) {
        Probe ordinary = probe(NULLIFY + "FROM " + A + commands + " | WHERE " + field + suffix + " | KEEP key | LIMIT 10");
        Probe sequence = probe(
            NULLIFY
                + "SEQUENCE BY key WITHIN 10s PRIMARY 2 (FROM "
                + A
                + commands
                + "),(FROM "
                + B
                + " | WHERE MEMBER(1,"
                + field
                + ")"
                + suffix
                + " | KEEP key) | KEEP key | LIMIT 10"
        );
        assertParity(field, ordinary, sequence);
    }

    private static void assertParity(String field, Probe ordinary, Probe sequence) {
        assertEquals(
            "ordinary reference versus MEMBER: " + ordinary + " / " + sequence,
            ordinary.verificationFailure(),
            sequence.verificationFailure()
        );
        if (ordinary.verificationFailure()) {
            assertThat(sequence.error(), containsString(field));
        } else {
            assertEquals(ordinary.rows(), sequence.rows());
        }
    }

    private Probe probe(String query) {
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            return new Probe(false, getValuesList(response), null);
        } catch (Exception failure) {
            Throwable cause = ExceptionsHelper.unwrapCause(failure);
            assertThat("unexpected execution failure for " + query, cause, instanceOf(VerificationException.class));
            return new Probe(true, null, cause.getMessage());
        }
    }

    /** Invalid references and successful reads must both drain the source lifetime. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    private record Probe(boolean verificationFailure, List<List<Object>> rows, String error) {}
}
