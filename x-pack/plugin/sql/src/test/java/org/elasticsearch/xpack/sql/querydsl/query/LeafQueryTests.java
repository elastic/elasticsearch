/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class LeafQueryTests extends ESTestCase {
    private static class DummyLeafQuery extends LeafQuery {
        private DummyLeafQuery(Location location) {
            super(location);
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
        DummyLeafQuery query = new DummyLeafQuery(LocationTests.randomLocation());
        checkEqualsAndHashCode(query, LeafQueryTests::copy, LeafQueryTests::mutate);
    }

    private static DummyLeafQuery copy(DummyLeafQuery query) {
        return new DummyLeafQuery(query.location());
    }

    private static DummyLeafQuery mutate(DummyLeafQuery query) {
        return new DummyLeafQuery(LocationTests.mutate(query.location()));
    }

    public void testContainsNestedField() {
        Query query = new DummyLeafQuery(LocationTests.randomLocation());
        // Leaf queries don't contain nested fields.
        assertFalse(query.containsNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5)));
    }

    public void testAddNestedField() {
        Query query = new DummyLeafQuery(LocationTests.randomLocation());
        // Leaf queries don't contain nested fields.
        assertSame(query, query.addNestedField(randomAlphaOfLength(5), randomAlphaOfLength(5), DocValueFieldsContext.USE_DEFAULT_FORMAT,
                randomBoolean()));
    }

    public void testEnrichNestedSort() {
        Query query = new DummyLeafQuery(LocationTests.randomLocation());
        // Leaf queries don't contain nested fields.
        NestedSortBuilder sort = new NestedSortBuilder(randomAlphaOfLength(5));
        query.enrichNestedSort(sort);
        assertNull(sort.getFilter());
    }

    public void testToString() {
        assertEquals("DummyLeafQuery@1:2[]", new DummyLeafQuery(new Location(1, 1)).toString());
    }
}
