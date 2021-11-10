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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TypeQueryBuilderTests extends AbstractQueryTestCase<TypeQueryBuilder> {

    @Override
    protected TypeQueryBuilder doCreateTestQueryBuilder() {
        return new TypeQueryBuilder("_doc");
    }

    @Override
    protected void doAssertLuceneQuery(TypeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (createSearchExecutionContext().getType().equals(queryBuilder.type())) {
            assertThat(query, equalTo(Queries.newNonNestedFilter(context.indexVersionCreated())));
        } else {
            assertEquals(new MatchNoDocsQuery(), query);
        }
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new TypeQueryBuilder((String) null));
    }

    public void testFromJson() throws IOException {
        String json = "{\n" + "  \"type\" : {\n" + "    \"value\" : \"my_type\",\n" + "    \"boost\" : 1.0\n" + "  }\n" + "}";

        TypeQueryBuilder parsed = (TypeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "my_type", parsed.type());
    }

    @Override
    public void testToQuery() throws IOException {
        super.testToQuery();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testMustRewrite() throws IOException {
        super.testMustRewrite();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testCacheability() throws IOException {
        super.testCacheability();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }
}
