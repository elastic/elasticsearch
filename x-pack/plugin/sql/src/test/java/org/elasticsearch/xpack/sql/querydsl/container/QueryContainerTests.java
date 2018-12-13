/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import java.util.AbstractMap.SimpleImmutableEntry;

import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class QueryContainerTests extends ESTestCase {
    private Location location = LocationTests.randomLocation();
    private String path = randomAlphaOfLength(5);
    private String name = randomAlphaOfLength(5);
    private String format = DocValueFieldsContext.USE_DEFAULT_FORMAT;
    private boolean hasDocValues = randomBoolean();

    public void testRewriteToContainNestedFieldNoQuery() {
        Query expected = new NestedQuery(location, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                new MatchAll(location));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(null, location, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenContainsNestedField() {
        Query original = new BoolQuery(location, true,
            new NestedQuery(location, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(location)),
            new RangeQuery(location, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean()));
        assertSame(original, QueryContainer.rewriteToContainNestedField(original, location, path, name, format, randomBoolean()));
    }

    public void testRewriteToContainsNestedFieldWhenCanAddNestedField() {
        Query buddy = new RangeQuery(location, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query original = new BoolQuery(location, true,
            new NestedQuery(location, path, emptyMap(), new MatchAll(location)),
            buddy);
        Query expected = new BoolQuery(location, true,
            new NestedQuery(location, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(location)),
            buddy);
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, location, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenDoesNotContainNestedFieldAndCantAdd() {
        Query original = new RangeQuery(location, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query expected = new BoolQuery(location, true,
            original,
            new NestedQuery(location, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(location)));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, location, path, name, format, hasDocValues));
    }
}
