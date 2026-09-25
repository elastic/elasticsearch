/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.columnar.ColumnarStringAutomatonQuery;
import org.elasticsearch.columnar.ColumnarStringMatchQuery;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

/**
 * Which query each format answers a shape with. The results these produce are checked against each other elsewhere, over
 * real documents; what this pins is that the columnar format reaches the column at all. Nothing else would notice if it
 * stopped: the scanning path answers every one of these correctly, only by reading a value for every document, so a
 * regression that quietly routed a columnar field back to it would leave every other test green.
 */
public class BinaryDocValuesQueriesTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** Every shape, and the query the column answers it with. */
    private static List<Shape> shapes() {
        return List.of(
            new Shape("term", q -> q.term(FIELD, new BytesRef("a")), ColumnarStringTermQuery.class),
            new Shape("terms", q -> q.terms(FIELD, List.of(new BytesRef("a"), new BytesRef("b"))), ColumnarStringMatchQuery.class),
            new Shape("range", q -> q.range(FIELD, new BytesRef("a"), new BytesRef("b"), true, false), ColumnarStringMatchQuery.class),
            // A prefix is a run the column can bisect, so it never becomes an automaton.
            new Shape("prefix", q -> q.prefix(FIELD, "a", false), ColumnarStringTermQuery.class),
            new Shape("prefix ci", q -> q.prefix(FIELD, "a", true), ColumnarStringAutomatonQuery.class),
            new Shape("fuzzy", q -> q.fuzzy(FIELD, "abc", 1, 0, true), ColumnarStringAutomatonQuery.class),
            new Shape("term ci", q -> q.caseInsensitiveTerm(FIELD, "a"), ColumnarStringAutomatonQuery.class),
            // A pattern naming a whole value, a prefix or a contained run is rewritten to the query that answers it.
            new Shape("wildcard literal", q -> q.wildcard(FIELD, "abc", false), ColumnarStringTermQuery.class),
            new Shape("wildcard prefix", q -> q.wildcard(FIELD, "abc*", false), ColumnarStringTermQuery.class),
            new Shape("wildcard contains", q -> q.wildcard(FIELD, "*abc*", false), ColumnarStringTermQuery.class),
            new Shape("wildcard", q -> q.wildcard(FIELD, "a*b*c", false), ColumnarStringAutomatonQuery.class),
            new Shape("wildcard ci", q -> q.wildcard(FIELD, "a*b", true), ColumnarStringAutomatonQuery.class),
            new Shape("regexp", q -> q.regexp(FIELD, "a.*", 0, 0, 10000, null), ColumnarStringAutomatonQuery.class),
            new Shape("automaton", q -> q.automaton(FIELD, Automata.makeString("a"), "described"), ColumnarStringAutomatonQuery.class)
        );
    }

    private record Shape(String name, Function<BinaryDocValuesQueries, Query> build, Class<? extends Query> columnar) {}

    public void testColumnarFormatReachesTheColumn() {
        final BinaryDocValuesQueries queries = BinaryDocValuesQueries.forFormat(BinaryDocValuesFormat.COLUMNAR_PAYLOAD);
        for (Shape shape : shapes()) {
            final Query query = shape.build().apply(queries);
            assertEquals(shape.name(), shape.columnar(), query.getClass());
        }
    }

    /** The formats that carry a blob per document keep reading one, whatever the shape. */
    public void testBlobFormatsScan() {
        for (BinaryDocValuesFormat format : List.of(BinaryDocValuesFormat.SEPARATE_COUNT, BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL)) {
            final BinaryDocValuesQueries queries = BinaryDocValuesQueries.forFormat(format);
            for (Shape shape : shapes()) {
                final Query query = shape.build().apply(queries);
                assertFalse(
                    format + " " + shape.name() + " reached the column",
                    query.getClass().getPackageName().startsWith("org.elasticsearch.columnar")
                );
            }
        }
    }

    /**
     * Every parameter that changes which documents a pattern matches has to reach {@link Query#equals}, or two
     * queries that ask different things share a cache entry and a {@code bool} keeping both collapses them into one.
     * An automaton carries what it was built from, which is what makes this hold however the description is spelt.
     */
    public void testPatternQueriesCompareOnWhatTheyMatch() {
        final BinaryDocValuesQueries queries = BinaryDocValuesQueries.forFormat(BinaryDocValuesFormat.COLUMNAR_PAYLOAD);
        assertEquals(queries.fuzzy(FIELD, "alpha", 1, 0, true), queries.fuzzy(FIELD, "alpha", 1, 0, true));
        assertNotEquals(queries.fuzzy(FIELD, "alpha", 1, 0, true), queries.fuzzy(FIELD, "alpha", 2, 0, true));
        // A prefix the edit distance may not touch, so the two accept different values.
        assertNotEquals(queries.fuzzy(FIELD, "alpha", 1, 0, true), queries.fuzzy(FIELD, "alpha", 1, 3, true));
        // Whether a transposition counts as one edit or two, so "alhpa" is accepted by one and not the other.
        assertNotEquals(queries.fuzzy(FIELD, "alpha", 1, 0, true), queries.fuzzy(FIELD, "alpha", 1, 0, false));
        assertNotEquals(queries.wildcard(FIELD, "a*b", false), queries.wildcard(FIELD, "a*c", false));
        assertNotEquals(queries.wildcard(FIELD, "a*b", false), queries.wildcard(FIELD, "a*b", true));
        assertNotEquals(queries.prefix(FIELD, "abc", true), queries.prefix(FIELD, "abd", true));
        assertNotEquals(queries.caseInsensitiveTerm(FIELD, "abc"), queries.caseInsensitiveTerm(FIELD, "abd"));
        assertNotEquals(queries.regexp(FIELD, "a.*", 0, 0, 10000, null), queries.regexp(FIELD, "b.*", 0, 0, 10000, null));
    }

    /** A columnar field is answered by its column, so asking for a scan of one fails where it is asked. */
    public void testAColumnHasNoScanningQuery() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ScanningBinaryDocValuesQueries.forFormat(BinaryDocValuesFormat.COLUMNAR_PAYLOAD)
        );
        assertThat(e.getMessage(), containsString("not by scanning"));
    }

    /** A description stands in for a predicate, so two equal queries share a cache entry. */
    public void testQueriesWithNoNaturalEqualityStillCompare() {
        final BinaryDocValuesQueries queries = BinaryDocValuesQueries.forFormat(BinaryDocValuesFormat.COLUMNAR_PAYLOAD);
        assertEquals(
            queries.terms(FIELD, List.of(new BytesRef("a"), new BytesRef("b"))),
            queries.terms(FIELD, List.of(new BytesRef("b"), new BytesRef("a")))
        );
        assertNotEquals(queries.terms(FIELD, List.of(new BytesRef("a"))), queries.terms(FIELD, List.of(new BytesRef("b"))));
        assertEquals(
            queries.range(FIELD, new BytesRef("a"), new BytesRef("b"), true, false),
            queries.range(FIELD, new BytesRef("a"), new BytesRef("b"), true, false)
        );
        assertNotEquals(
            queries.range(FIELD, new BytesRef("a"), new BytesRef("b"), true, false),
            queries.range(FIELD, new BytesRef("a"), new BytesRef("b"), true, true)
        );
    }
}
