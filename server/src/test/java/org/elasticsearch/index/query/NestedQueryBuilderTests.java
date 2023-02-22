/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.elasticsearch.index.query.InnerHitBuilderTests.randomNestedInnerHits;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.toString(
                    PutMappingRequest.simpleMapping(
                        TEXT_FIELD_NAME,
                        "type=text",
                        INT_FIELD_NAME,
                        "type=integer",
                        DOUBLE_FIELD_NAME,
                        "type=double",
                        BOOLEAN_FIELD_NAME,
                        "type=boolean",
                        DATE_FIELD_NAME,
                        "type=date",
                        OBJECT_FIELD_NAME,
                        "type=object",
                        GEO_POINT_FIELD_NAME,
                        "type=geo_point",
                        "nested1",
                        "type=nested"
                    )
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    /**
     * @return a {@link NestedQueryBuilder} with random values all over the place
     */
    @Override
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQueryBuilder = RandomQueryBuilder.createQuery(random());
        NestedQueryBuilder nqb = new NestedQueryBuilder("nested1", innerQueryBuilder, RandomPicks.randomFrom(random(), ScoreMode.values()));
        nqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            nqb.innerHit(
                new InnerHitBuilder(randomAlphaOfLengthBetween(1, 10)).setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(INT_FIELD_NAME).order(SortOrder.ASC))
                    .setIgnoreUnmapped(nqb.ignoreUnmapped())
            );
        }
        return nqb;
    }

    @Override
    protected NestedQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        return new NestedQueryBuilder("path", queryBuilder, ScoreMode.None);
    }

    @Override
    protected void doAssertLuceneQuery(NestedQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(ESToParentBlockJoinQuery.class));
        // TODO how to assert this?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (NestedQueryBuilder) queryBuilder.rewrite(context);

            assertNotNull(context);
            Map<String, InnerHitContextBuilder> innerHitInternals = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitInternals);
            assertTrue(innerHitInternals.containsKey(queryBuilder.innerHit().getName()));
            InnerHitContextBuilder innerHits = innerHitInternals.get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.innerHitBuilder(), queryBuilder.innerHit());
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (TransportVersion version : TransportVersionUtils.allReleasedVersions()) {
            NestedQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testValidate() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> QueryBuilders.nestedQuery(null, innerQuery, ScoreMode.Avg)
        );
        assertThat(e.getMessage(), equalTo("[nested] requires 'path' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", null, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'query' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", innerQuery, null));
        assertThat(e.getMessage(), equalTo("[nested] requires 'score_mode' field"));
    }

    public void testParseDefaultsRemoved() throws IOException {
        /*
         * This json includes many defaults. When we parse the query and then
         * call toString on it all of the defaults are removed.
         */
        String json = """
            {
              "nested" : {
                "query" : {
                  "bool" : {
                    "must" : [ {
                      "match" : {
                        "obj1.name" : {
                          "query" : "blue",
                          "operator" : "OR",
                          "prefix_length" : 0,
                          "max_expansions" : 50,
                          "fuzzy_transpositions" : true,
                          "lenient" : false,
                          "zero_terms_query" : "NONE",
                          "auto_generate_synonyms_phrase_query" : true,
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "range" : {
                        "obj1.count" : {
                          "gt" : 5,
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "boost" : 1.0
                  }
                },
                "path" : "obj1",
                "ignore_unmapped" : false,
                "score_mode" : "avg",
                "boost" : 1.0
              }
            }""";

        NestedQueryBuilder parsed = (NestedQueryBuilder) parseQuery(json);
        checkGeneratedJson("""
              {
              "nested" : {
                "query" : {
                  "bool" : {
                    "must" : [ {
                      "match" : {
                        "obj1.name" : {
                          "query" : "blue"
                        }
                      }
                    }, {
                      "range" : {
                        "obj1.count" : {
                          "gt" : 5,
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "boost" : 1.0
                  }
                },
                "path" : "obj1",
                "ignore_unmapped" : false,
                "score_mode" : "avg",
                "boost" : 1.0
              }
            }""", parsed);

        assertEquals(json, ScoreMode.Avg, parsed.scoreMode());
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder innerQueryBuilder = new TermQueryBuilder("nested1.unmapped_field", "foo");
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
            "nested1",
            innerQueryBuilder,
            RandomPicks.randomFrom(random(), ScoreMode.values())
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> nestedQueryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testIgnoreUnmapped() throws IOException {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final NestedQueryBuilder failingQueryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.ignoreUnmapped(false);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> failingQueryBuilder.toQuery(createSearchExecutionContext())
        );
        assertThat(e.getMessage(), containsString("[" + NestedQueryBuilder.NAME + "] failed to find nested object under path [unmapped]"));
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder(
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

    public void testMinFromString() {
        assertThat("fromString(min) != MIN", ScoreMode.Min, equalTo(NestedQueryBuilder.parseScoreMode("min")));
        assertThat("min", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Min)));
    }

    public void testMaxFromString() {
        assertThat("fromString(max) != MAX", ScoreMode.Max, equalTo(NestedQueryBuilder.parseScoreMode("max")));
        assertThat("max", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Max)));
    }

    public void testAvgFromString() {
        assertThat("fromString(avg) != AVG", ScoreMode.Avg, equalTo(NestedQueryBuilder.parseScoreMode("avg")));
        assertThat("avg", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Avg)));
    }

    public void testSumFromString() {
        assertThat("fromString(total) != SUM", ScoreMode.Total, equalTo(NestedQueryBuilder.parseScoreMode("sum")));
        assertThat("sum", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Total)));
    }

    public void testNoneFromString() {
        assertThat("fromString(none) != NONE", ScoreMode.None, equalTo(NestedQueryBuilder.parseScoreMode("none")));
        assertThat("none", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.None)));
    }

    /**
     * Should throw {@link IllegalArgumentException} instead of NPE.
     */
    public void testThatNullFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NestedQueryBuilder.parseScoreMode(null));
        assertEquals("No score mode for child query [null] found", e.getMessage());
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    public void testThatUnrecognizedFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilder.parseScoreMode("unrecognized value")
        );
        assertEquals("No score mode for child query [unrecognized value] found", e.getMessage());
    }

    public void testInlineLeafInnerHitsNestedQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        nestedQueryBuilder.innerHit(leafInnerHits);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        nestedQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoolQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().should(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        boolQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaConstantScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoostingQuery() {
        InnerHitBuilder leafInnerHits1 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits1
        );
        InnerHitBuilder leafInnerHits2 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits2
        );
        BoostingQueryBuilder constantScoreQueryBuilder = new BoostingQueryBuilder(nestedQueryBuilder1, nestedQueryBuilder2);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits1.getName()), Matchers.notNullValue());
        assertThat(innerHitBuilders.get(leafInnerHits2.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaFunctionScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        ((AbstractQueryBuilder<?>) functionScoreQueryBuilder).extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testBuildIgnoreUnmappedNestQuery() throws Exception {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        IndexSettings settings = new IndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        when(searchExecutionContext.getIndexSettings()).thenReturn(settings);
        when(searchExecutionContext.nestedLookup()).thenReturn(NestedLookup.EMPTY);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getSearchExecutionContext()).thenReturn(searchExecutionContext);
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder query1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query1.innerHit(leafInnerHits);
        final Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        final InnerHitsContext innerHitsContext = new InnerHitsContext();
        expectThrows(IllegalStateException.class, () -> {
            query1.extractInnerHitBuilders(innerHitBuilders);
            assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
            assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
            innerHitBuilders.get(leafInnerHits.getName()).build(searchContext, innerHitsContext);
        });
        innerHitBuilders.clear();
        NestedQueryBuilder query2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query2.ignoreUnmapped(true);
        query2.innerHit(leafInnerHits);
        query2.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
        assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), instanceOf(NestedQueryBuilder.NestedInnerHitContextBuilder.class));
        NestedQueryBuilder.NestedInnerHitContextBuilder nestedContextBuilder =
            (NestedQueryBuilder.NestedInnerHitContextBuilder) innerHitBuilders.get(leafInnerHits.getName());
        nestedContextBuilder.build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), Matchers.equalTo(0));
    }

    public void testExtractInnerHitBuildersWithDuplicate() {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder(
            "path",
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

        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
