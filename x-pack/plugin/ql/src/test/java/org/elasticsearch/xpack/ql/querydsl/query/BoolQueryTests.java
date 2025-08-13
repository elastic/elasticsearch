/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class BoolQueryTests extends ESTestCase {
    static BoolQuery randomBoolQuery(int depth) {
        return new BoolQuery(
            SourceTests.randomSource(),
            randomBoolean(),
            NestedQueryTests.randomQuery(depth),
            NestedQueryTests.randomQuery(depth)
        );
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomBoolQuery(5), BoolQueryTests::copy, BoolQueryTests::mutate);
    }

    private static BoolQuery copy(BoolQuery query) {
        return new BoolQuery(query.source(), query.isAnd(), query.queries());
    }

    private static BoolQuery mutate(BoolQuery query) {
        List<Function<BoolQuery, BoolQuery>> options = Arrays.asList(
            q -> new BoolQuery(SourceTests.mutate(q.source()), q.isAnd(), left(q), right(q)),
            q -> new BoolQuery(q.source(), false == q.isAnd(), left(q), right(q)),
            q -> new BoolQuery(q.source(), q.isAnd(), randomValueOtherThan(left(q), () -> NestedQueryTests.randomQuery(5)), right(q)),
            q -> new BoolQuery(q.source(), q.isAnd(), left(q), randomValueOtherThan(right(q), () -> NestedQueryTests.randomQuery(5)))
        );
        return randomFrom(options).apply(query);
    }

    public void testContainsNestedField() {
        assertFalse(boolQueryWithoutNestedChildren().containsNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5)));

        String path = randomAlphaOfLength(5);
        String field = randomAlphaOfLength(5);
        assertTrue(boolQueryWithNestedChildren(path, field).containsNestedField(path, field));
    }

    public void testAddNestedField() {
        Query q = boolQueryWithoutNestedChildren();
        assertSame(q, q.addNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5), null, randomBoolean()));

        String path = randomAlphaOfLength(5);
        String field = randomAlphaOfLength(5);
        q = boolQueryWithNestedChildren(path, field);
        String newField = randomAlphaOfLength(5);
        boolean hasDocValues = randomBoolean();
        Query rewritten = q.addNestedField(path, newField, null, hasDocValues);
        assertNotSame(q, rewritten);
        assertTrue(rewritten.containsNestedField(path, newField));
    }

    public void testEnrichNestedSort() {
        Query q = boolQueryWithoutNestedChildren();
        NestedSortBuilder sort = new NestedSortBuilder(randomAlphaOfLength(5));
        q.enrichNestedSort(sort);
        assertNull(sort.getFilter());

        String path = randomAlphaOfLength(5);
        String field = randomAlphaOfLength(5);
        q = boolQueryWithNestedChildren(path, field);
        sort = new NestedSortBuilder(path);
        q.enrichNestedSort(sort);
        assertNotNull(sort.getFilter());
    }

    private Query boolQueryWithoutNestedChildren() {
        return new BoolQuery(
            SourceTests.randomSource(),
            randomBoolean(),
            new MatchAll(SourceTests.randomSource()),
            new MatchAll(SourceTests.randomSource())
        );
    }

    private Query boolQueryWithNestedChildren(String path, String field) {
        NestedQuery match = new NestedQuery(
            SourceTests.randomSource(),
            path,
            singletonMap(field, new SimpleImmutableEntry<>(randomBoolean(), null)),
            new MatchAll(SourceTests.randomSource())
        );
        Query matchAll = new MatchAll(SourceTests.randomSource());
        Query left;
        Query right;
        if (randomBoolean()) {
            left = match;
            right = matchAll;
        } else {
            left = matchAll;
            right = match;
        }
        return new BoolQuery(SourceTests.randomSource(), randomBoolean(), left, right);
    }

    public void testToString() {
        assertEquals(
            "BoolQuery@1:2[ExistsQuery@1:2[f1] AND ExistsQuery@1:8[f2]]",
            new BoolQuery(
                new Source(1, 1, StringUtils.EMPTY),
                true,
                new ExistsQuery(new Source(1, 1, StringUtils.EMPTY), "f1"),
                new ExistsQuery(new Source(1, 7, StringUtils.EMPTY), "f2")
            ).toString()
        );
    }

    public void testNotAllNegated() {
        var q = new BoolQuery(Source.EMPTY, true, new ExistsQuery(Source.EMPTY, "f1"), new ExistsQuery(Source.EMPTY, "f2"));
        assertThat(q.negate(Source.EMPTY), equalTo(new NotQuery(Source.EMPTY, q)));
    }

    public void testNotSomeNegated() {
        var q = new BoolQuery(
            Source.EMPTY,
            true,
            new ExistsQuery(Source.EMPTY, "f1"),
            new NotQuery(Source.EMPTY, new ExistsQuery(Source.EMPTY, "f2"))
        );
        assertThat(
            q.negate(Source.EMPTY),
            equalTo(
                new BoolQuery(
                    Source.EMPTY,
                    false,
                    new NotQuery(Source.EMPTY, new ExistsQuery(Source.EMPTY, "f1")),
                    new ExistsQuery(Source.EMPTY, "f2")
                )
            )
        );
    }

    public static Query left(BoolQuery bool) {
        return indexOf(bool, 0);
    }

    public static Query right(BoolQuery bool) {
        return indexOf(bool, 1);
    }

    private static Query indexOf(BoolQuery bool, int index) {
        List<Query> queries = bool.queries();
        assertThat(queries, hasSize(greaterThanOrEqualTo(2)));
        return queries.get(index);
    }

}
