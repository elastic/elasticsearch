/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParentIdQueryBuilderTests extends AbstractQueryTestCase<ParentIdQueryBuilder> {

    private static final String TYPE = "_doc";
    private static final String JOIN_FIELD_NAME = "join_field";
    private static final String PARENT_NAME = "parent";
    private static final String CHILD_NAME = "child";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ParentJoinPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("join_field")
            .field("type", "join")
            .startObject("relations")
            .field("parent", "child")
            .endObject()
            .endObject()
            .startObject(TEXT_FIELD_NAME)
            .field("type", "text")
            .endObject()
            .startObject(KEYWORD_FIELD_NAME)
            .field("type", "keyword")
            .endObject()
            .startObject(INT_FIELD_NAME)
            .field("type", "integer")
            .endObject()
            .startObject(DOUBLE_FIELD_NAME)
            .field("type", "double")
            .endObject()
            .startObject(BOOLEAN_FIELD_NAME)
            .field("type", "boolean")
            .endObject()
            .startObject(DATE_FIELD_NAME)
            .field("type", "date")
            .endObject()
            .startObject(OBJECT_FIELD_NAME)
            .field("type", "object")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        mapperService.merge(TYPE, new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected ParentIdQueryBuilder doCreateTestQueryBuilder() {
        return new ParentIdQueryBuilder(CHILD_NAME, randomAlphaOfLength(4)).ignoreUnmapped(randomBoolean());
    }

    @Override
    protected void doAssertLuceneQuery(ParentIdQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, Matchers.instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), Matchers.equalTo(2));
        BooleanQuery expected = new BooleanQuery.Builder().add(
            new TermQuery(new Term(JOIN_FIELD_NAME + "#" + PARENT_NAME, queryBuilder.getId())),
            BooleanClause.Occur.MUST
        ).add(new TermQuery(new Term(JOIN_FIELD_NAME, queryBuilder.getType())), BooleanClause.Occur.FILTER).build();
        assertThat(expected, equalTo(query));
    }

    public void testFromJson() throws IOException {
        String query = """
            {
              "parent_id" : {
                "type" : "child",
                "id" : "123",
                "ignore_unmapped" : true,
                "boost" : 3.0,
                "_name" : "name"  }
            }""";
        ParentIdQueryBuilder queryBuilder = (ParentIdQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertThat(queryBuilder.getType(), Matchers.equalTo("child"));
        assertThat(queryBuilder.getId(), Matchers.equalTo("123"));
        assertThat(queryBuilder.boost(), Matchers.equalTo(3f));
        assertThat(queryBuilder.queryName(), Matchers.equalTo("name"));
    }

    public void testDefaultsRemoved() throws IOException {
        String query = """
            {
              "parent_id" : {
                "type" : "child",
                "id" : "123",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        checkGeneratedJson("""
              {
              "parent_id" : {
                "type" : "child",
                "id" : "123"
              }
            }""", parseQuery(query));
    }

    public void testIgnoreUnmapped() throws IOException {
        final ParentIdQueryBuilder queryBuilder = new ParentIdQueryBuilder("unmapped", "foo");
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final ParentIdQueryBuilder failingQueryBuilder = new ParentIdQueryBuilder("unmapped", "foo");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("[" + ParentIdQueryBuilder.NAME + "] no relation found for child [unmapped]"));
    }

    public void testThrowsOnNullTypeOrId() {
        expectThrows(IllegalArgumentException.class, () -> new ParentIdQueryBuilder(null, randomAlphaOfLength(5)));
        expectThrows(IllegalArgumentException.class, () -> new ParentIdQueryBuilder(randomAlphaOfLength(5), null));
    }

    public void testDisallowExpensiveQueries() {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(false);

        ParentIdQueryBuilder queryBuilder = doCreateTestQueryBuilder();
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
