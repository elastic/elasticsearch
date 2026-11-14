/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class QueryFingerprintTests extends ESTestCase {

    public void testParametersThatDecideTheQualityAreInTheIdentityAndTheLabelIsNot() {
        float[] vector = randomVector(16);
        CapturedQuery base = new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, null, List.of(), "x");

        assertThat("only a label", QueryFingerprint.of(base), equalTo(QueryFingerprint.of(withLabel(base, "y"))));
        for (CapturedQuery other : List.of(
            new CapturedQuery(new String[] { "a" }, "vec", vector, 50, 100, null, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 500, null, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, 0.1f, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, 3f, List.of(), "x")
        )) {
            assertThat(QueryFingerprint.of(other), not(equalTo(QueryFingerprint.of(base))));
        }
    }

    public void testFiltersAreInTheIdentityButTheirOrderIsNot() {
        float[] vector = randomVector(16);
        QueryBuilder category = QueryBuilders.termQuery("category", 3);
        QueryBuilder price = QueryBuilders.rangeQuery("price").lt(10);
        QueryBuilder tag = QueryBuilders.termQuery("tag", "a");

        QueryFingerprint ordered = QueryFingerprint.of(withFilters(vector, List.of(category, price, tag)));

        assertThat("filters are ANDed", QueryFingerprint.of(withFilters(vector, List.of(tag, category, price))), equalTo(ordered));
        assertThat(QueryFingerprint.of(withFilters(vector, List.of(price, tag, category))), equalTo(ordered));
        assertThat("another filter", QueryFingerprint.of(withFilters(vector, List.of(category, price))), not(equalTo(ordered)));
        assertThat("no filter", QueryFingerprint.of(withFilters(vector, List.of())), not(equalTo(ordered)));
        assertThat(
            "another value of a filter",
            QueryFingerprint.of(withFilters(vector, List.of(QueryBuilders.termQuery("category", 4), price, tag))),
            not(equalTo(ordered))
        );
    }

    private static CapturedQuery withFilters(float[] vector, List<QueryBuilder> filters) {
        return new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, null, filters, "x");
    }

    private static CapturedQuery withLabel(CapturedQuery query, String label) {
        return new CapturedQuery(
            query.indices(),
            query.field(),
            query.queryVector(),
            query.k(),
            query.numCandidates(),
            query.visitPercentage(),
            query.oversample(),
            query.filters(),
            label
        );
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
