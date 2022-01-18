/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.util.StringUtils;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class LeafQueryTests extends ESTestCase {
    private static class DummyLeafQuery extends LeafQuery {
        private DummyLeafQuery(Source source) {
            super(source);
        }

        @Override
        public QueryBuilder asBuilder() {
            return null;
        }

        @Override
        protected String innerToString() {
            return "";
        }
    }

    public void testEqualsAndHashCode() {
        DummyLeafQuery query = new DummyLeafQuery(SourceTests.randomSource());
        checkEqualsAndHashCode(query, LeafQueryTests::copy, LeafQueryTests::mutate);
    }

    private static DummyLeafQuery copy(DummyLeafQuery query) {
        return new DummyLeafQuery(query.source());
    }

    private static DummyLeafQuery mutate(DummyLeafQuery query) {
        return new DummyLeafQuery(SourceTests.mutate(query.source()));
    }

    public void testContainsNestedField() {
        Query query = new DummyLeafQuery(SourceTests.randomSource());
        // Leaf queries don't contain nested fields.
        assertFalse(query.containsNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5)));
    }

    public void testAddNestedField() {
        Query query = new DummyLeafQuery(SourceTests.randomSource());
        // Leaf queries don't contain nested fields.
        assertSame(query, query.addNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5), null, randomBoolean()));
    }

    public void testEnrichNestedSort() {
        Query query = new DummyLeafQuery(SourceTests.randomSource());
        // Leaf queries don't contain nested fields.
        NestedSortBuilder sort = new NestedSortBuilder(randomAlphaOfLength(5));
        query.enrichNestedSort(sort);
        assertNull(sort.getFilter());
    }

    public void testToString() {
        assertEquals("DummyLeafQuery@1:2[]", new DummyLeafQuery(new Source(1, 1, StringUtils.EMPTY)).toString());
    }
}
