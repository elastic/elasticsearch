/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;

import java.util.Arrays;
import java.util.List;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static java.util.Collections.singletonMap;

public class BoolQueryTests extends ESTestCase {
    static BoolQuery randomBoolQuery(int depth) {
        return new BoolQuery(LocationTests.randomLocation(), randomBoolean(),
                NestedQueryTests.randomQuery(depth), NestedQueryTests.randomQuery(depth));
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomBoolQuery(5), BoolQueryTests::copy, BoolQueryTests::mutate);
    }

    private static BoolQuery copy(BoolQuery query) {
        return new BoolQuery(query.location(), query.isAnd(), query.left(), query.right());
    }

    private static BoolQuery mutate(BoolQuery query) {
        List<Function<BoolQuery, BoolQuery>> options = Arrays.asList(
            q -> new BoolQuery(LocationTests.mutate(q.location()), q.isAnd(), q.left(), q.right()),
            q -> new BoolQuery(q.location(), false == q.isAnd(), q.left(), q.right()),
            q -> new BoolQuery(q.location(), q.isAnd(), randomValueOtherThan(q.left(), () -> NestedQueryTests.randomQuery(5)), q.right()),
            q -> new BoolQuery(q.location(), q.isAnd(), q.left(), randomValueOtherThan(q.right(), () -> NestedQueryTests.randomQuery(5))));
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
        assertSame(q, q.addNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5), DocValueFieldsContext.USE_DEFAULT_FORMAT,
                randomBoolean()));

        String path = randomAlphaOfLength(5);
        String field = randomAlphaOfLength(5);
        q = boolQueryWithNestedChildren(path, field);
        String newField = randomAlphaOfLength(5);
        boolean hasDocValues = randomBoolean();
        Query rewritten = q.addNestedField(path, newField, DocValueFieldsContext.USE_DEFAULT_FORMAT, hasDocValues);
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
        return new BoolQuery(LocationTests.randomLocation(), randomBoolean(),
            new MatchAll(LocationTests.randomLocation()), new MatchAll(LocationTests.randomLocation()));
    }

    private Query boolQueryWithNestedChildren(String path, String field) {
        NestedQuery match = new NestedQuery(LocationTests.randomLocation(), path,
                singletonMap(field, new SimpleImmutableEntry<>(randomBoolean(), DocValueFieldsContext.USE_DEFAULT_FORMAT)),
                new MatchAll(LocationTests.randomLocation()));
        Query matchAll = new MatchAll(LocationTests.randomLocation());
        Query left;
        Query right;
        if (randomBoolean()) {
            left = match;
            right = matchAll;
        } else {
            left = matchAll;
            right = match;
        }
        return new BoolQuery(LocationTests.randomLocation(), randomBoolean(), left, right);
    }

    public void testToString() {
        assertEquals("BoolQuery@1:2[ExistsQuery@1:2[f1] AND ExistsQuery@1:8[f2]]",
                new BoolQuery(new Location(1, 1), true,
                    new ExistsQuery(new Location(1, 1), "f1"),
                    new ExistsQuery(new Location(1, 7), "f2")).toString());
    }
}
