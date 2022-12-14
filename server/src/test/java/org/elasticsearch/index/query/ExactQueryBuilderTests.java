/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.TextFieldExactQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ExactQueryBuilderTests extends AbstractQueryTestCase<ExactQueryBuilder> {

    @Override
    protected ExactQueryBuilder doCreateTestQueryBuilder() {
        return switch (random().nextInt(5)) {
            case 0 -> new ExactQueryBuilder("mapped_string", "here is some text");
            case 1 -> new ExactQueryBuilder("mapped_string_2", "category");
            case 2 -> new ExactQueryBuilder("mapped_int", 1);
            case 3 -> new ExactQueryBuilder("mapped_double", 0.5d);
            case 4 -> new ExactQueryBuilder("mapped_date", "2022-12-12T00:00:00Z");
            default -> throw new IllegalArgumentException("Impossible");
        };
    }

    @Override
    protected void doAssertLuceneQuery(ExactQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        switch (queryBuilder.getField()) {
            case "mapped_string" -> assertThat(query, instanceOf(TextFieldExactQuery.class));
            case "mapped_string_2" -> assertThat(query, instanceOf(ConstantScoreQuery.class));
            case "mapped_int", "mapped_double", "mapped_date" -> assertThat(query, instanceOf(ConstantScoreQuery.class));
        }
    }

    public void testSimpleXContent() throws IOException {
        String input = """
            { "exact" : { "field" : "value" } }
            """;
        ExactQueryBuilder query = (ExactQueryBuilder) parseQuery(input);
        assertEquals(new ExactQueryBuilder("field", "value"), query);
    }
}
