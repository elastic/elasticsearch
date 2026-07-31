/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class DerivedMetricsPredicateTests extends ESTestCase {

    private static final Map<String, Object> DOCUMENT = Map.of(
        "event",
        Map.of("outcome", "failure", "duration", 1500),
        "http",
        Map.of("request", Map.of("method", "GET"), "response", Map.of("status_code", 503)),
        "tags",
        List.of("alpha", "beta")
    );

    public void testExists() {
        assertTrue(matches(Map.of("exists", Map.of("field", "event.outcome"))));
        assertFalse(matches(Map.of("exists", Map.of("field", "event.missing"))));
    }

    public void testTerm() {
        assertTrue(matches(Map.of("term", Map.of("event.outcome", "failure"))));
        assertFalse(matches(Map.of("term", Map.of("event.outcome", "success"))));
        assertFalse(matches(Map.of("term", Map.of("event.missing", "failure"))));
    }

    public void testTermMatchesAnyValueOfAMultiValuedField() {
        assertTrue(matches(Map.of("term", Map.of("tags", "beta"))));
        assertFalse(matches(Map.of("term", Map.of("tags", "gamma"))));
    }

    /**
     * A source document does not have to agree with the mapping, so a numeric field written as a string still matches a numeric term.
     */
    public void testTermIsLenientAboutNumericTypes() {
        assertTrue(
            matches(
                Map.of("term", Map.of("http.response.status_code", 503)),
                Map.of("http", Map.of("response", Map.of("status_code", "503")))
            )
        );
    }

    public void testTerms() {
        assertTrue(matches(Map.of("terms", Map.of("http.response.status_code", List.of(500, 502, 503)))));
        assertFalse(matches(Map.of("terms", Map.of("http.response.status_code", List.of(500, 502)))));
    }

    public void testRange() {
        assertTrue(matches(Map.of("range", Map.of("event.duration", Map.of("gt", 0, "lte", 1500)))));
        assertFalse(matches(Map.of("range", Map.of("event.duration", Map.of("gt", 1500)))));
        assertTrue(matches(Map.of("range", Map.of("event.duration", Map.of("gte", 1500)))));
        assertFalse(matches(Map.of("range", Map.of("event.duration", Map.of("lt", 1500)))));
        assertFalse(matches(Map.of("range", Map.of("event.missing", Map.of("gt", 0)))));
    }

    public void testRangeIgnoresNonNumericValues() {
        assertFalse(matches(Map.of("range", Map.of("event.outcome", Map.of("gt", 0)))));
    }

    public void testAnd() {
        assertTrue(
            matches(
                Map.of(
                    "and",
                    List.of(Map.of("exists", Map.of("field", "event.duration")), Map.of("term", Map.of("event.outcome", "failure")))
                )
            )
        );
        assertFalse(
            matches(
                Map.of(
                    "and",
                    List.of(Map.of("exists", Map.of("field", "event.duration")), Map.of("term", Map.of("event.outcome", "success")))
                )
            )
        );
    }

    public void testOr() {
        assertTrue(
            matches(
                Map.of(
                    "or",
                    List.of(Map.of("term", Map.of("event.outcome", "success")), Map.of("term", Map.of("event.outcome", "failure")))
                )
            )
        );
        assertFalse(matches(Map.of("or", List.of(Map.of("term", Map.of("event.outcome", "success"))))));
    }

    public void testNot() {
        assertTrue(matches(Map.of("not", Map.of("term", Map.of("event.outcome", "success")))));
        assertFalse(matches(Map.of("not", Map.of("term", Map.of("event.outcome", "failure")))));
    }

    public void testNullPredicateMatchesEverything() {
        assertSame(DerivedMetricsPredicate.MATCH_ALL, DerivedMetricsPredicate.compile(null, new DerivedMetricsSourcePaths()));
        assertTrue(DerivedMetricsPredicate.MATCH_ALL.test(new Object[0]));
    }

    /**
     * Compiling a predicate is also what claims a slot for every field it reads, so that evaluation is an array read rather than a path
     * lookup. Nothing else walks the predicate tree looking for paths any more.
     */
    public void testCompilingClaimsASlotForEveryFieldRead() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        DerivedMetricsPredicate.compile(
            Map.of(
                "and",
                List.of(
                    Map.of("exists", Map.of("field", "event.duration")),
                    Map.of("not", Map.of("term", Map.of("event.outcome", "success"))),
                    Map.of(
                        "or",
                        List.of(
                            Map.of("terms", Map.of("http.response.status_code", List.of(500))),
                            Map.of("range", Map.of("http.request.bytes", Map.of("gt", 0)))
                        )
                    )
                )
            ),
            paths
        );
        assertEquals(
            Set.of("event.duration", "event.outcome", "http.response.status_code", "http.request.bytes"),
            Set.copyOf(paths.paths())
        );
    }

    /**
     * Two fields naming the same path share one slot, so the value is extracted once per document however many predicates want it.
     */
    public void testTheSameFieldClaimsTheSameSlot() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        DerivedMetricsPredicate.compile(
            Map.of("and", List.of(Map.of("term", Map.of("event.outcome", "failure")), Map.of("exists", Map.of("field", "event.outcome")))),
            paths
        );
        assertEquals(1, paths.size());
    }

    public void testUnsupportedOperator() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DerivedMetricsPredicate.compile(Map.of("wildcard", Map.of("event.outcome", "fail*")), new DerivedMetricsSourcePaths())
        );
        assertThat(e.getMessage(), containsString("unsupported derived metrics predicate operator [wildcard]"));
    }

    private static boolean matches(Map<String, Object> predicate) {
        return matches(predicate, DOCUMENT);
    }

    /**
     * Compiles the predicate, extracts what the document has at the paths it claimed, and evaluates it — the same sequence the write path
     * runs, so the extractor and the predicate are exercised together rather than one being faked for the other.
     */
    private static boolean matches(Map<String, Object> predicate, Map<String, Object> document) {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        DerivedMetricsPredicate compiled = DerivedMetricsPredicate.compile(predicate, paths);
        Object[] values = new Object[paths.size()];
        DerivedMetricsSourceReader.read(DerivedMetricsSourceReaderTests.parsedDocument(document), paths, values);
        return compiled.test(values);
    }
}
