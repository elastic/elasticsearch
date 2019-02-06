/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.SourceTests;

import java.util.AbstractMap.SimpleImmutableEntry;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class QueryContainerTests extends ESTestCase {
    private Source source = SourceTests.randomSource();
    private String path = randomAlphaOfLength(5);
    private String name = randomAlphaOfLength(5);
    private String format = null;
    private boolean hasDocValues = randomBoolean();

    public void testRewriteToContainNestedFieldNoQuery() {
        Query expected = new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                new MatchAll(source));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(null, source, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenContainsNestedField() {
        Query original = new BoolQuery(source, true,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)),
            new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean()));
        assertSame(original, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, randomBoolean()));
    }

    public void testRewriteToContainsNestedFieldWhenCanAddNestedField() {
        Query buddy = new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query original = new BoolQuery(source, true,
            new NestedQuery(source, path, emptyMap(), new MatchAll(source)),
            buddy);
        Query expected = new BoolQuery(source, true,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)),
            buddy);
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenDoesNotContainNestedFieldAndCantAdd() {
        Query original = new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query expected = new BoolQuery(source, true,
            original,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, hasDocValues));
    }
}
