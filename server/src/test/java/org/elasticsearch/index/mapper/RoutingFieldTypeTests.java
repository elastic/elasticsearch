/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;

public class RoutingFieldTypeTests extends FieldTypeTestCase {

    public void testPrefixQuery() {
        MappedFieldType ft = RoutingFieldMapper.RoutingFieldType.INSTANCE;

        Query expected = new PrefixQuery(new Term("_routing", new BytesRef("foo*")));
        assertEquals(expected, ft.prefixQuery("foo*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.prefixQuery("foo*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals("[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. " +
                "For optimised prefix queries on text fields please enable [index_prefixes].", ee.getMessage());
    }

    public void testRegexpQuery() {
        MappedFieldType ft = RoutingFieldMapper.RoutingFieldType.INSTANCE;

        Query expected = new RegexpQuery(new Term("_routing", new BytesRef("foo?")));
        assertEquals(expected, ft.regexpQuery("foo?", 0, 0, 10, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.regexpQuery("foo?", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                ee.getMessage());
    }

    public void testWildcardQuery() {
        MappedFieldType ft = RoutingFieldMapper.RoutingFieldType.INSTANCE;

        Query expected = new WildcardQuery(new Term("_routing", new BytesRef("foo*")));
        assertEquals(expected, ft.wildcardQuery("foo*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.wildcardQuery("valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                ee.getMessage());
    }
}
