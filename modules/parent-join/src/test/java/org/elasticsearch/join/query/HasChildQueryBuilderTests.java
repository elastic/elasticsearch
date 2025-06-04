/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.similarity.SimilarityService;
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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HasChildQueryBuilderTests extends AbstractQueryTestCase<HasChildQueryBuilder> {

    private static final String TYPE = "_doc";
    private static final String PARENT_DOC = "parent";
    private static final String CHILD_DOC = "child";

    private static String similarity;

    boolean requiresRewrite = false;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ParentJoinPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        similarity = randomFrom("boolean", "BM25");
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
            .startObject("custom_string")
            .field("type", "text")
            .field("similarity", similarity)
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
    protected HasChildQueryBuilder doCreateTestQueryBuilder() {
        int min = randomIntBetween(1, Integer.MAX_VALUE / 2);
        int max = randomIntBetween(min, Integer.MAX_VALUE);

        QueryBuilder innerQueryBuilder = new MatchAllQueryBuilder();
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }

        HasChildQueryBuilder hqb = new HasChildQueryBuilder(
            CHILD_DOC,
            innerQueryBuilder,
            RandomPicks.randomFrom(random(), ScoreMode.values())
        );
        hqb.minMaxChildren(min, max);
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
    protected HasChildQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        return new HasChildQueryBuilder("type", queryBuilder, ScoreMode.None);
    }

    @Override
    protected void doAssertLuceneQuery(HasChildQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(LateParsingQuery.class));
        LateParsingQuery lpq = (LateParsingQuery) query;
        assertEquals(queryBuilder.minChildren(), lpq.getMinChildren());
        assertEquals(queryBuilder.maxChildren(), lpq.getMaxChildren());
        assertEquals(queryBuilder.scoreMode(), lpq.getScoreMode()); // WTF is this why do we have two?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (HasChildQueryBuilder) queryBuilder.rewrite(context);
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
            HasChildQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testIllegalValues() {
        QueryBuilder query = new MatchAllQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery(null, query, ScoreMode.None));
        assertEquals("[has_child] requires 'type' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery("foo", null, ScoreMode.None));
        assertEquals("[has_child] requires 'query' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery("foo", query, null));
        assertEquals("[has_child] requires 'score_mode' field", e.getMessage());

        int positiveValue = randomIntBetween(0, Integer.MAX_VALUE);
        HasChildQueryBuilder foo = hasChildQuery("foo", query, ScoreMode.None); // all good
        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(randomIntBetween(Integer.MIN_VALUE, -1), positiveValue));
        assertEquals("[has_child] requires positive 'min_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, randomIntBetween(Integer.MIN_VALUE, -1)));
        assertEquals("[has_child] requires positive 'max_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, positiveValue - 10));
        assertEquals("[has_child] 'max_children' is less than 'min_children'", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String query = """
            {
              "has_child" : {
                "query" : {
                  "range" : {
                    "mapped_string" : {
                      "gte" : "agJhRET",
                      "lte" : "zvqIq",
                      "boost" : 1.0
                    }
                  }
                },
                "type" : "child",
                "score_mode" : "avg",
                "min_children" : 883170873,
                "max_children" : 1217235442,
                "ignore_unmapped" : false,
                "boost" : 2.0,
                "_name" : "WNzYMJKRwePuRBh",
                "inner_hits" : {
                  "name" : "inner_hits_name",
                  "size" : 100,
                  "sort" : [ {
                    "mapped_string" : {
                      "order" : "asc"
                    }
                  } ]
                }
              }
            }""";
        HasChildQueryBuilder queryBuilder = (HasChildQueryBuilder) parseQuery(query);
        checkGeneratedJson(
            /*
             * Ignoring unmapped is the default and we don't dump it and can't
             * change it if we're going to use inner_hits.
             */
            query.replaceAll("\"ignore_unmapped\" : false,", ""),
            queryBuilder
        );
        assertEquals(query, queryBuilder.maxChildren(), 1217235442);
        assertEquals(query, queryBuilder.minChildren(), 883170873);
        assertEquals(query, queryBuilder.boost(), 2.0f, 0.0f);
        assertEquals(query, queryBuilder.queryName(), "WNzYMJKRwePuRBh");
        assertEquals(query, queryBuilder.childType(), "child");
        assertEquals(query, queryBuilder.scoreMode(), ScoreMode.Avg);
        assertNotNull(query, queryBuilder.innerHit());
        InnerHitBuilder expected = new InnerHitBuilder("child").setName("inner_hits_name")
            .setSize(100)
            .addSort(new FieldSortBuilder("mapped_string").order(SortOrder.ASC));
        assertEquals(query, queryBuilder.innerHit(), expected);
    }

    public void testParseDefaultsRemoved() throws IOException {
        String query = """
            {
              "has_child" : {
                "query" : {
                  "range" : {
                    "mapped_string" : {
                      "gte" : "agJhRET",
                      "lte" : "zvqIq",
                      "boost" : 1.0
                    }
                  }
                },
                "type" : "child",
                "score_mode" : "none",
                "min_children" : 1,
                "max_children" : MAX_CHILDREN,
                "ignore_unmapped" : false,
                "boost" : 1.0,
                "inner_hits" : {
                  "name" : "inner_hits_name",
                  "ignore_unmapped" : false,
                  "from" : 0,
                  "size" : 100,
                  "version" : false,
                  "seq_no_primary_term" : false,
                  "explain" : false,
                  "track_scores" : false,
                  "sort" : [ {
                    "mapped_string" : {
                      "order" : "asc"
                    }
                  } ]
                }
              }
            }""".replaceAll("MAX_CHILDREN", Integer.toString(Integer.MAX_VALUE));
        checkGeneratedJson("""
            {
              "has_child" : {
                "query" : {
                  "range" : {
                    "mapped_string" : {
                      "gte" : "agJhRET",
                      "lte" : "zvqIq",
                      "boost" : 1.0
                    }
                  }
                },
                "type" : "child",
                "inner_hits" : {
                  "name" : "inner_hits_name",
                  "size" : 100,
                  "sort" : [ {
                    "mapped_string" : {
                      "order" : "asc"
                    }
                  } ]
                }
              }
            }""", parseQuery(query));
    }

    public void testToQueryInnerQueryType() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        HasChildQueryBuilder hasChildQueryBuilder = hasChildQuery(CHILD_DOC, new IdsQueryBuilder().addIds("id"), ScoreMode.None);
        Query query = hasChildQueryBuilder.toQuery(searchExecutionContext);
        assertLateParsingQuery(query, CHILD_DOC, "id");
    }

    static void assertLateParsingQuery(Query query, String type, String id) throws IOException {
        assertThat(query, instanceOf(LateParsingQuery.class));
        LateParsingQuery lateParsingQuery = (LateParsingQuery) query;
        assertThat(lateParsingQuery.getInnerQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) lateParsingQuery.getInnerQuery();
        assertThat(booleanQuery.clauses().size(), equalTo(2));
        // check the inner ids query, we have to call rewrite to get to check the type it's executed against
        assertThat(booleanQuery.clauses().get(0).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(booleanQuery.clauses().get(0).query(), instanceOf(TermInSetQuery.class));
        TermInSetQuery termsQuery = (TermInSetQuery) booleanQuery.clauses().get(0).query();
        assertEquals(new TermInSetQuery(IdFieldMapper.NAME, List.of(Uid.encodeId(id))), termsQuery);
        // check the type filter
        assertThat(booleanQuery.clauses().get(1).occur(), equalTo(BooleanClause.Occur.FILTER));
        assertEquals(new TermQuery(new Term("join_field", type)), booleanQuery.clauses().get(1).query());
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

    public void testNonDefaultSimilarity() throws Exception {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        HasChildQueryBuilder hasChildQueryBuilder = hasChildQuery(
            CHILD_DOC,
            new TermQueryBuilder("custom_string", "value"),
            ScoreMode.None
        );
        LateParsingQuery query = (LateParsingQuery) hasChildQueryBuilder.toQuery(searchExecutionContext);
        Similarity expected = SimilarityService.BUILT_IN.get(similarity).apply(Settings.EMPTY, IndexVersion.current(), null);
        assertThat(((PerFieldSimilarityWrapper) query.getSimilarity()).get("custom_string"), instanceOf(expected.getClass()));
    }

    public void testIgnoreUnmapped() throws IOException {
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(queryBuilder.innerHit().isIgnoreUnmapped());
        queryBuilder.ignoreUnmapped(true);
        assertTrue(queryBuilder.innerHit().isIgnoreUnmapped());
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final HasChildQueryBuilder failingQueryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        failingQueryBuilder.ignoreUnmapped(false);
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(
            e.getMessage(),
            containsString("[" + HasChildQueryBuilder.NAME + "] join field [join_field] doesn't hold [unmapped] as a child")
        );
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder(
            "unmapped",
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            ScoreMode.None
        );
        queryBuilder.ignoreUnmapped(true);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        Query query = queryBuilder.rewrite(searchExecutionContext).toQuery(searchExecutionContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testExtractInnerHitBuildersWithDuplicate() {
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder(
            CHILD_DOC,
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            ScoreMode.None
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

        HasChildQueryBuilder queryBuilder = hasChildQuery(CHILD_DOC, new TermQueryBuilder("custom_string", "value"), ScoreMode.None);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
