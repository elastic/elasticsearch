/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ExistsQueryBuilderTests extends AbstractQueryTestCase<ExistsQueryBuilder> {
    @Override
    protected ExistsQueryBuilder doCreateTestQueryBuilder() {
        String fieldPattern;
        if (randomBoolean()) {
            fieldPattern = randomFrom(MAPPED_FIELD_NAMES);
        } else {
            fieldPattern = randomAlphaOfLengthBetween(1, 10);
        }
        // also sometimes test wildcard patterns
        if (randomBoolean()) {
            if (randomBoolean()) {
                fieldPattern = fieldPattern + "*";
            }
        }
        return new ExistsQueryBuilder(fieldPattern);
    }

    @Override
    protected void doAssertLuceneQuery(ExistsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        String fieldPattern = queryBuilder.fieldName();
        Collection<String> fields = context.getMatchingFieldNames(fieldPattern);
        if (fields.size() == 0 && Regex.isSimpleMatchPattern(fieldPattern) == false) {
            if (context.getMatchingFieldNames(fieldPattern + ".*").isEmpty() == false) {
                // we're an object field
                fields = Collections.singleton(fieldPattern);
            }
        }
        if (fields.size() == 0) {
            assertThat(fieldPattern, query, instanceOf(MatchNoDocsQuery.class));
        } else if (fields.size() == 1) {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            String field = expectedFieldName(fields.iterator().next());
            if (context.getFieldType(field) == null) {
                // not a leaf field, so we're doing an object exists query
                assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) constantScoreQuery.getQuery();
                Collection<String> childFields = context.getMatchingFieldNames(field + ".*");
                assertThat(booleanQuery.clauses().size(), equalTo(childFields.size()));
                for (BooleanClause booleanClause : booleanQuery) {
                    assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
                }
            } else if (context.getFieldType(field).hasDocValues() || context.getFieldType(field).getTextSearchInfo().hasNorms()) {
                assertThat(constantScoreQuery.getQuery(), instanceOf(FieldExistsQuery.class));
                FieldExistsQuery existsQuery = (FieldExistsQuery) constantScoreQuery.getQuery();
                assertEquals(field, existsQuery.getField());
            } else {
                assertThat(constantScoreQuery.getQuery(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) constantScoreQuery.getQuery();
                assertEquals(field, termQuery.getTerm().text());
            }
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
            BooleanQuery booleanQuery = (BooleanQuery) constantScoreQuery.getQuery();
            assertThat(booleanQuery.clauses().size(), equalTo(fields.size()));
            for (int i = 0; i < fields.size(); i++) {
                BooleanClause booleanClause = booleanQuery.clauses().get(i);
                assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
            }
        }
    }

    @Override
    public void testMustRewrite() {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        ExistsQueryBuilder queryBuilder = new ExistsQueryBuilder("foo");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
        Query ret = ExistsQueryBuilder.newFilter(context, "foo", false);
        assertThat(ret, instanceOf(MatchNoDocsQuery.class));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ExistsQueryBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new ExistsQueryBuilder(""));
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "exists" : {
                "field" : "user",
                "boost" : 42.0
              }
            }""";

        ExistsQueryBuilder parsed = (ExistsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 42.0, parsed.boost(), 0.0001);
        assertEquals(json, "user", parsed.fieldName());
    }
}
