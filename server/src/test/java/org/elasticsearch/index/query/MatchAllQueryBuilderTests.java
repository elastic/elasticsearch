/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MatchAllQueryBuilderTests extends AbstractQueryTestCase<MatchAllQueryBuilder> {

    @Override
    protected MatchAllQueryBuilder doCreateTestQueryBuilder() {
        return new MatchAllQueryBuilder();
    }

    @Override
    protected void doAssertLuceneQuery(MatchAllQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"match_all\" : {\n" +
                "    \"boost\" : 1.2\n" +
                "  }\n" +
                "}";
        MatchAllQueryBuilder parsed = (MatchAllQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 1.2, parsed.boost(), 0.0001);
    }
}
