/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Builds each charged query through the Lucene factory Elasticsearch uses and checks that {@link ExecutionEstimators} charges it,
 * so that a Lucene upgrade that changes the class a factory returns fails here rather than going uncharged.
 */
public class ExecutionEstimatorsTests extends ESTestCase {

    private static final String FIELD = "f";

    public void testPointRange() {
        assertCharged(LongPoint.newRangeQuery(FIELD, 1, 10));
    }

    public void testMultiTerm() {
        assertCharged(new WildcardQuery(new Term(FIELD, "a*b")));
    }

    public void testPointInSet() {
        assertCharged(LongPoint.newSetQuery(FIELD, 1, 5, 7));
        assertCharged(IntPoint.newSetQuery(FIELD, 1, 5, 7));
        assertCharged(InetAddressPoint.newSetQuery(FIELD, InetAddresses.forString("10.0.0.1"), InetAddresses.forString("::1")));
    }

    public void testRangeField() {
        long[] min = { 1 };
        long[] max = { 10 };
        assertCharged(LongRange.newIntersectsQuery(FIELD, min, max));
        assertCharged(LongRange.newWithinQuery(FIELD, min, max));
        assertCharged(LongRange.newContainsQuery(FIELD, min, max));
        assertCharged(LongRange.newCrossesQuery(FIELD, min, max));
        assertCharged(IntRange.newIntersectsQuery(FIELD, new int[] { 1 }, new int[] { 10 }));
    }

    public void testRangeFieldIsFoundThroughVisit() {
        assertThat(ExecutionEstimators.fieldOf(LongRange.newIntersectsQuery(FIELD, new long[] { 1 }, new long[] { 10 })), equalTo(FIELD));
    }

    public void testBoostAndIndexOrDocValuesAreUnwrapped() {
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 1, 10);
        for (Query indexQuery : new Query[] {
            LongPoint.newRangeQuery(FIELD, 1, 10),
            LongPoint.newSetQuery(FIELD, 1, 5, 7),
            LongRange.newIntersectsQuery(FIELD, new long[] { 1 }, new long[] { 10 }) }) {
            assertCharged(new BoostQuery(indexQuery, 2f));
            assertCharged(new IndexOrDocValuesQuery(indexQuery, dvQuery));
            assertCharged(new BoostQuery(new IndexOrDocValuesQuery(indexQuery, dvQuery), 2f));
        }
    }

    public void testNotCharged() {
        assertNotCharged(new TermQuery(new Term(FIELD, "a")));
        assertNotCharged(MatchAllDocsQuery.INSTANCE);
        assertNotCharged(SortedNumericDocValuesField.newSlowSetQuery(FIELD, 1, 5));
        // ConstantScoreQuery creates its child's weight through the searcher, which looks the child up on its own.
        assertNotCharged(new ConstantScoreQuery(LongPoint.newSetQuery(FIELD, 1, 5)));
    }

    private static void assertCharged(Query query) {
        assertThat("no charge for " + query, ExecutionEstimators.forQuery(query), notNullValue());
    }

    private static void assertNotCharged(Query query) {
        assertThat("unexpected charge for " + query, ExecutionEstimators.forQuery(query), nullValue());
    }
}
