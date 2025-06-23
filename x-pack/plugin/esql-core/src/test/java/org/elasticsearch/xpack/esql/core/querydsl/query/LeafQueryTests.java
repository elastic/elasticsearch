/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class LeafQueryTests extends ESTestCase {
    private static class DummyLeafQuery extends Query {
        private DummyLeafQuery(Source source) {
            super(source);
        }

        @Override
        protected QueryBuilder asBuilder() {
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

    public void testNot() {
        var q = new LeafQueryTests.DummyLeafQuery(new Source(Location.EMPTY, "test"));
        assertThat(q.negate(new Source(Location.EMPTY, "not")), equalTo(new NotQuery(new Source(Location.EMPTY, "not"), q)));
    }

    public void testNotNot() {
        var q = new LeafQueryTests.DummyLeafQuery(new Source(Location.EMPTY, "test"));
        assertThat(q.negate(Source.EMPTY).negate(Source.EMPTY), equalTo(q));
    }

    public void testToString() {
        assertEquals("DummyLeafQuery@1:2[]", new DummyLeafQuery(new Source(1, 1, StringUtils.EMPTY)).toString());
    }
}
