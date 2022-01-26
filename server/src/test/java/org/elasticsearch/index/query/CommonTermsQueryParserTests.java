/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class CommonTermsQueryParserTests extends ESSingleNodeTestCase {
    public void testWhenParsedQueryIsNullNoNullPointerExceptionIsThrown() {
        final String index = "test-index";
        final String type = "test-type";
        client().admin().indices().prepareCreate(index).addMapping(type, "name", "type=text,analyzer=stop").execute().actionGet();
        ensureGreen();

        CommonTermsQueryBuilder commonTermsQueryBuilder = new CommonTermsQueryBuilder("name", "the").queryName("query-name");

        // the named query parses to null; we are testing this does not cause a NullPointerException
        SearchResponse response = client().prepareSearch(index).setTypes(type).setQuery(commonTermsQueryBuilder).execute().actionGet();

        assertNotNull(response);
        assertEquals(response.getHits().getHits().length, 0);
    }
}
