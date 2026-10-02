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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class QueryFingerprintTests extends ESTestCase {

    private static final QueryBuilder BRAND = QueryBuilders.termQuery("brand", "apple");
    private static final QueryBuilder CATEGORY = QueryBuilders.termQuery("category", "phone");

    public void testSameQueryHasSameFingerprint() {
        float[] vector = randomVector(16);
        assertThat(
            QueryFingerprint.of(query("vec", vector, List.of(BRAND))),
            equalTo(QueryFingerprint.of(query("vec", vector.clone(), List.of(BRAND))))
        );
    }

    public void testRuntimeParametersAreNotPartOfTheIdentity() {
        float[] vector = randomVector(16);
        CapturedQuery a = new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, null, List.of(), "x");
        CapturedQuery b = new CapturedQuery(new String[] { "a" }, "vec", vector, 50, 500, 0.1f, 3f, List.of(), "y");
        assertThat(QueryFingerprint.of(a), equalTo(QueryFingerprint.of(b)));
    }

    public void testFilterOrderDoesNotMatter() {
        float[] vector = randomVector(16);
        assertThat(
            QueryFingerprint.of(query("vec", vector, List.of(BRAND, CATEGORY))),
            equalTo(QueryFingerprint.of(query("vec", vector, List.of(CATEGORY, BRAND))))
        );
    }

    public void testIndicesArePartOfTheIdentityButTheirOrderIsNot() {
        float[] vector = randomVector(16);
        CapturedQuery a = new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, null, List.of(), null);
        CapturedQuery b = new CapturedQuery(new String[] { "b" }, "vec", vector, 10, 100, null, null, List.of(), null);
        CapturedQuery ab = new CapturedQuery(new String[] { "a", "b" }, "vec", vector, 10, 100, null, null, List.of(), null);
        CapturedQuery ba = new CapturedQuery(new String[] { "b", "a" }, "vec", vector, 10, 100, null, null, List.of(), null);

        assertThat(QueryFingerprint.of(a), not(equalTo(QueryFingerprint.of(b))));
        assertThat(QueryFingerprint.of(a), not(equalTo(QueryFingerprint.of(ab))));
        assertThat(QueryFingerprint.of(ab), equalTo(QueryFingerprint.of(ba)));
    }

    public void testDifferentQueriesHaveDifferentFingerprints() {
        float[] vector = randomVector(16);
        QueryFingerprint base = QueryFingerprint.of(query("vec", vector, List.of(BRAND)));

        float[] other = vector.clone();
        other[between(0, other.length - 1)] += 1f;
        assertThat(QueryFingerprint.of(query("vec", other, List.of(BRAND))), not(equalTo(base)));
        assertThat(QueryFingerprint.of(query("other_vec", vector, List.of(BRAND))), not(equalTo(base)));
        assertThat(QueryFingerprint.of(query("vec", vector, List.of(CATEGORY))), not(equalTo(base)));
        assertThat(QueryFingerprint.of(query("vec", vector, List.of(BRAND, CATEGORY))), not(equalTo(base)));
        assertThat(QueryFingerprint.of(query("vec", vector, List.of())), not(equalTo(base)));
    }

    public void testNoCollisionsAmongManyQueries() {
        Set<QueryFingerprint> seen = new HashSet<>();
        int queries = 50_000;
        for (int i = 0; i < queries; i++) {
            seen.add(QueryFingerprint.of(query("vec", randomVector(8), List.of())));
        }
        assertThat(seen.size(), equalTo(queries));
    }

    private static CapturedQuery query(String field, float[] vector, List<QueryBuilder> filters) {
        return new CapturedQuery(new String[] { "idx" }, field, vector, 10, 100, null, null, filters, null);
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
