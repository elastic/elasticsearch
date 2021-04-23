/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MatchNoneQueryBuilderTests extends AbstractQueryTestCase<MatchNoneQueryBuilder> {

    @Override
    protected MatchNoneQueryBuilder doCreateTestQueryBuilder() {
        return new MatchNoneQueryBuilder();
    }

    @Override
    protected void doAssertLuceneQuery(MatchNoneQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"match_none\" : {\n" +
                "    \"boost\" : 1.2\n" +
                "  }\n" +
                "}";
        MatchNoneQueryBuilder parsed = (MatchNoneQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 1.2, parsed.boost(), 0.0001);
    }
}
