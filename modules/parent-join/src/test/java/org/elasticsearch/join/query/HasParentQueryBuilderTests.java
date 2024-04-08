/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.query.HasChildQueryBuilder.LateParsingQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HasParentQueryBuilderTests extends AbstractQueryTestCase<HasParentQueryBuilder> {
    private static final String TYPE = "_doc";
    private static final String PARENT_DOC = "parent";
    private static final String CHILD_DOC = "child";

    boolean requiresRewrite = false;

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
            .field(PARENT_DOC, CHILD_DOC)
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

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasParentQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQueryBuilder = new MatchAllQueryBuilder();
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }
        HasParentQueryBuilder hqb = new HasParentQueryBuilder(PARENT_DOC, innerQueryBuilder, randomBoolean());
        hqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            hqb.innerHit(
                new InnerHitBuilder().setName(randomAlphaOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(KEYWORD_FIELD_NAME).order(SortOrder.ASC))
            );
        }
        return hqb;
    }

    @Override
    protected HasParentQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        return new HasParentQueryBuilder("type", queryBuilder, randomBoolean());
    }

    @Override
    protected void doAssertLuceneQuery(HasParentQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(LateParsingQuery.class));
        LateParsingQuery lpq = (LateParsingQuery) query;
        assertEquals(queryBuilder.score() ? ScoreMode.Max : ScoreMode.None, lpq.getScoreMode());

        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (HasParentQueryBuilder) queryBuilder.rewrite(context);

            assertNotNull(context);
            Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
            assertTrue(innerHitBuilders.containsKey(queryBuilder.innerHit().getName()));
            InnerHitContextBuilder innerHits = innerHitBuilders.get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.innerHitBuilder(), queryBuilder.innerHit());
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (TransportVersion version : TransportVersionUtils.allReleasedVersions()) {
            HasParentQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testIllegalValues() throws IOException {
        QueryBuilder query = new MatchAllQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> hasParentQuery(null, query, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'parent_type' field"));

        e = expectThrows(IllegalArgumentException.class, () -> hasParentQuery("foo", null, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'query' field"));

        SearchExecutionContext context = createSearchExecutionContext();
        HasParentQueryBuilder qb = hasParentQuery("just_a_type", new MatchAllQueryBuilder(), false);
        QueryShardException qse = expectThrows(QueryShardException.class, () -> qb.doToQuery(context));
        assertThat(qse.getMessage(), equalTo("[has_parent] join field [join_field] doesn't hold [just_a_type] as a parent"));
    }

    public void testToQueryInnerQueryType() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(PARENT_DOC, new IdsQueryBuilder().addIds("id"), false);
        Query query = hasParentQueryBuilder.toQuery(searchExecutionContext);
        HasChildQueryBuilderTests.assertLateParsingQuery(query, PARENT_DOC, "id");
    }

    @Override
    public void testMustRewrite() throws IOException {
        try {
            super.testMustRewrite();
        } catch (UnsupportedOperationException e) {
            if (requiresRewrite == false) {
                throw e;
            }
        }
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "has_parent" : {
                "query" : {
                  "term" : {
                    "tag" : {
                      "value" : "something"
                    }
                  }
                },
                "parent_type" : "blog",
                "score" : true,
                "ignore_unmapped" : true,
                "boost" : 2.0
              }
            }""";
        HasParentQueryBuilder parsed = (HasParentQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "blog", parsed.type());
        assertEquals(json, "something", ((TermQueryBuilder) parsed.query()).value());
        assertEquals(json, true, parsed.ignoreUnmapped());
    }

    public void testParseDefaultsRemoved() throws IOException {
        String json = """
            {
              "has_parent" : {
                "query" : {
                  "term" : {
                    "tag" : {
                      "value" : "something"
                    }
                  }
                },
                "parent_type" : "blog",
                "score" : false,
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        checkGeneratedJson("""
            {
              "has_parent" : {
                "query" : {
                  "term" : {
                    "tag" : {
                      "value" : "something"
                    }
                  }
                },
                "parent_type" : "blog"
              }
            }""", parseQuery(json));

    }

    public void testIgnoreUnmapped() throws IOException {
        final HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder("unmapped", new MatchAllQueryBuilder(), false);
        queryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(queryBuilder.innerHit().isIgnoreUnmapped());
        queryBuilder.ignoreUnmapped(true);
        assertTrue(queryBuilder.innerHit().isIgnoreUnmapped());
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final HasParentQueryBuilder failingQueryBuilder = new HasParentQueryBuilder("unmapped", new MatchAllQueryBuilder(), false);
        failingQueryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        failingQueryBuilder.ignoreUnmapped(false);
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("[has_parent] join field [join_field] doesn't hold [unmapped] as a parent"));
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder(
            "unmapped",
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            false
        );
        queryBuilder.ignoreUnmapped(true);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        Query query = queryBuilder.rewrite(searchExecutionContext).toQuery(searchExecutionContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testExtractInnerHitBuildersWithDuplicate() {
        final HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder(
            CHILD_DOC,
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            false
        );
        queryBuilder.innerHit(new InnerHitBuilder("some_name"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InnerHitContextBuilder.extractInnerHits(queryBuilder, Collections.singletonMap("some_name", null))
        );
        assertEquals("[inner_hits] already contains an entry for key [some_name]", e.getMessage());
    }

    public void testDisallowExpensiveQueries() {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(false);

        HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder(
            CHILD_DOC,
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            false
        );
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
