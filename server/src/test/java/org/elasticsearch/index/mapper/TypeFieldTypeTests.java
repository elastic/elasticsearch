/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Arrays;

public class TypeFieldTypeTests extends ESTestCase {

    public void testTermsQuery() {
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);

        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType("_doc");
        Query query = ft.termQuery("my_type", context);
        assertEquals(new MatchNoDocsQuery(), query);

        query = ft.termQuery("_doc", context);
        assertEquals(new MatchAllDocsQuery(), query);

        query = ft.termsQuery(Arrays.asList("_doc", "type", "foo"), context);
        assertEquals(new MatchAllDocsQuery(), query);

        query = ft.termsQuery(Arrays.asList("type", "foo"), context);
        assertEquals(new MatchNoDocsQuery(), query);

        query = ft.termQueryCaseInsensitive("_DOC", context);
        assertEquals(new MatchAllDocsQuery(), query);

        assertWarnings("[types removal] Using the _type field in queries and aggregations is deprecated, prefer to use a field instead.");
    }
}
