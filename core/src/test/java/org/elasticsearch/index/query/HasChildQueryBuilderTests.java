/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class HasChildQueryBuilderTests extends AbstractQueryTestCase<HasChildQueryBuilder> {
    protected static final String PARENT_TYPE = "parent";
    protected static final String CHILD_TYPE = "child";

    private static String similarity;

    boolean requiresRewrite = false;

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        similarity = randomFrom("classic", "BM25");
        mapperService.merge(PARENT_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(PARENT_TYPE,
                STRING_FIELD_NAME, "type=text",
                STRING_FIELD_NAME_2, "type=keyword",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge(CHILD_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(CHILD_TYPE,
                "_parent", "type=" + PARENT_TYPE,
                STRING_FIELD_NAME, "type=text",
                "custom_string", "type=text,similarity=" + similarity,
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasChildQueryBuilder doCreateTestQueryBuilder() {
        int min = randomIntBetween(0, Integer.MAX_VALUE / 2);
        int max = randomIntBetween(min, Integer.MAX_VALUE);

        QueryBuilder innerQueryBuilder = RandomQueryBuilder.createQuery(random());
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }

        HasChildQueryBuilder hqb = new HasChildQueryBuilder(CHILD_TYPE, innerQueryBuilder,
                RandomPicks.randomFrom(random(), ScoreMode.values()));
        hqb.minMaxChildren(min, max);
        if (randomBoolean()) {
            hqb.innerHit(new InnerHitBuilder()
                    .setName(randomAsciiOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(STRING_FIELD_NAME_2).order(SortOrder.ASC)));
        }
        hqb.ignoreUnmapped(randomBoolean());
        return hqb;
    }

    @Override
    protected void doAssertLuceneQuery(HasChildQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
        assertEquals(queryBuilder.minChildren(), lpq.getMinChildren());
        assertEquals(queryBuilder.maxChildren(), lpq.getMaxChildren());
        assertEquals(queryBuilder.scoreMode(), lpq.getScoreMode()); // WTF is this why do we have two?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (HasChildQueryBuilder) queryBuilder.rewrite(context);
            SearchContext searchContext = SearchContext.current();
            assertNotNull(searchContext);
            Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
            InnerHitBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
            for (InnerHitBuilder builder : innerHitBuilders.values()) {
                builder.build(searchContext, searchContext.innerHits());
            }
            assertNotNull(searchContext.innerHits());
            assertEquals(1, searchContext.innerHits().getInnerHits().size());
            assertTrue(searchContext.innerHits().getInnerHits().containsKey(queryBuilder.innerHit().getName()));
            InnerHitsContext.BaseInnerHits innerHits =
                    searchContext.innerHits().getInnerHits().get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.size(), queryBuilder.innerHit().getSize());
            assertEquals(innerHits.sort().sort.getSort().length, 1);
            assertEquals(innerHits.sort().sort.getSort()[0].getField(), STRING_FIELD_NAME_2);
        }
    }

    public void testIllegalValues() {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> QueryBuilders.hasChildQuery(null, query, ScoreMode.None));
        assertEquals("[has_child] requires 'type' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.hasChildQuery("foo", null, ScoreMode.None));
        assertEquals("[has_child] requires 'query' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.hasChildQuery("foo", query, null));
        assertEquals("[has_child] requires 'score_mode' field", e.getMessage());

        int positiveValue = randomIntBetween(0, Integer.MAX_VALUE);
        HasChildQueryBuilder foo = QueryBuilders.hasChildQuery("foo", query, ScoreMode.None); // all good
        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(randomIntBetween(Integer.MIN_VALUE, -1), positiveValue));
        assertEquals("[has_child] requires non-negative 'min_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, randomIntBetween(Integer.MIN_VALUE, -1)));
        assertEquals("[has_child] requires non-negative 'max_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, positiveValue - 10));
        assertEquals("[has_child] 'max_children' is less than 'min_children'", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String query =
                "{\n" +
                "  \"has_child\" : {\n" +
                "    \"query\" : {\n" +
                "      \"range\" : {\n" +
                "        \"mapped_string\" : {\n" +
                "          \"from\" : \"agJhRET\",\n" +
                "          \"to\" : \"zvqIq\",\n" +
                "          \"include_lower\" : true,\n" +
                "          \"include_upper\" : true,\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"type\" : \"child\",\n" +
                "    \"score_mode\" : \"avg\",\n" +
                "    \"min_children\" : 883170873,\n" +
                "    \"max_children\" : 1217235442,\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 2.0,\n" +
                "    \"_name\" : \"WNzYMJKRwePuRBh\",\n" +
                "    \"inner_hits\" : {\n" +
                "      \"name\" : \"inner_hits_name\",\n" +
                "      \"from\" : 0,\n" +
                "      \"size\" : 100,\n" +
                "      \"version\" : false,\n" +
                "      \"explain\" : false,\n" +
                "      \"track_scores\" : false,\n" +
                "      \"sort\" : [ {\n" +
                "        \"mapped_string\" : {\n" +
                "          \"order\" : \"asc\"\n" +
                "        }\n" +
                "      } ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HasChildQueryBuilder queryBuilder = (HasChildQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertEquals(query, queryBuilder.maxChildren(), 1217235442);
        assertEquals(query, queryBuilder.minChildren(), 883170873);
        assertEquals(query, queryBuilder.boost(), 2.0f, 0.0f);
        assertEquals(query, queryBuilder.queryName(), "WNzYMJKRwePuRBh");
        assertEquals(query, queryBuilder.childType(), "child");
        assertEquals(query, queryBuilder.scoreMode(), ScoreMode.Avg);
        assertNotNull(query, queryBuilder.innerHit());
        InnerHitBuilder expected = new InnerHitBuilder(new InnerHitBuilder(), queryBuilder.query(), "child")
                .setName("inner_hits_name")
                .setSize(100)
                .addSort(new FieldSortBuilder("mapped_string").order(SortOrder.ASC));
        assertEquals(query, queryBuilder.innerHit(), expected);
    }

    /**
     * we resolve empty inner clauses by representing this whole query as empty optional upstream
     */
    public void testFromJsonEmptyQueryBody() throws IOException {
        String query =  "{\n" +
                "  \"has_child\" : {\n" +
                "    \"query\" : { },\n" +
                "    \"type\" : \"child\"" +
                "   }" +
                "}";
        XContentParser parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext context = createParseContext(parser, ParseFieldMatcher.EMPTY);
        Optional<QueryBuilder> innerQueryBuilder = context.parseInnerQueryBuilder();
        assertTrue(innerQueryBuilder.isPresent() == false);

        parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext otherContext = createParseContext(parser, ParseFieldMatcher.STRICT);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> otherContext.parseInnerQueryBuilder());
        assertThat(ex.getMessage(), startsWith("query malformed, empty clause found at"));
    }

    public void testToQueryInnerQueryType() throws IOException {
        String[] searchTypes = new String[]{PARENT_TYPE};
        QueryShardContext shardContext = createShardContext();
        shardContext.setTypes(searchTypes);
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(CHILD_TYPE, new IdsQueryBuilder().addIds("id"), ScoreMode.None);
        Query query = hasChildQueryBuilder.toQuery(shardContext);
        //verify that the context types are still the same as the ones we previously set
        assertThat(shardContext.getTypes(), equalTo(searchTypes));
        assertLateParsingQuery(query, CHILD_TYPE, "id");
    }

    static void assertLateParsingQuery(Query query, String type, String id) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lateParsingQuery = (HasChildQueryBuilder.LateParsingQuery) query;
        assertThat(lateParsingQuery.getInnerQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) lateParsingQuery.getInnerQuery();
        assertThat(booleanQuery.clauses().size(), equalTo(2));
        //check the inner ids query, we have to call rewrite to get to check the type it's executed against
        assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(booleanQuery.clauses().get(0).getQuery(), instanceOf(TermsQuery.class));
        TermsQuery termsQuery = (TermsQuery) booleanQuery.clauses().get(0).getQuery();
        Query rewrittenTermsQuery = termsQuery.rewrite(null);
        assertThat(rewrittenTermsQuery, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) rewrittenTermsQuery;
        assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanTermsQuery = (BooleanQuery) constantScoreQuery.getQuery();
        assertThat(booleanTermsQuery.clauses().toString(), booleanTermsQuery.clauses().size(), equalTo(1));
        assertThat(booleanTermsQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanTermsQuery.clauses().get(0).getQuery(), instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) booleanTermsQuery.clauses().get(0).getQuery();
        assertThat(termQuery.getTerm().field(), equalTo(UidFieldMapper.NAME));
        //we want to make sure that the inner ids query gets executed against the child type rather than the main type we initially set to the context
        BytesRef[] ids = Uid.createUidsForTypesAndIds(Collections.singletonList(type), Collections.singletonList(id));
        assertThat(termQuery.getTerm().bytes(), equalTo(ids[0]));
        //check the type filter
        assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.FILTER));
        assertEquals(new TypeFieldMapper.TypeQuery(new BytesRef(type)), booleanQuery.clauses().get(1).getQuery());
    }

    /**
     * override superclass test, because here we need to take care that mutation doesn't happen inside
     * `inner_hits` structure, because we don't parse them yet and so no exception will be triggered
     * for any mutation there.
     */
    @Override
    public void testUnknownObjectException() throws IOException {
        String validQuery = createTestQueryBuilder().toString();
        assertThat(validQuery, containsString("{"));
        int endPosition = validQuery.indexOf("inner_hits");
        if (endPosition == -1) {
            endPosition = validQuery.length() - 1;
        }
        for (int insertionPosition = 0; insertionPosition < endPosition; insertionPosition++) {
            if (validQuery.charAt(insertionPosition) == '{') {
                String testQuery = validQuery.substring(0, insertionPosition) + "{ \"newField\" : " + validQuery.substring(insertionPosition) + "}";
                try {
                    parseQuery(testQuery);
                    fail("some parsing exception expected for query: " + testQuery);
                } catch (ParsingException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
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
        QueryShardContext shardContext = createShardContext();
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(CHILD_TYPE, new TermQueryBuilder("custom_string", "value"), ScoreMode.None);
        HasChildQueryBuilder.LateParsingQuery query = (HasChildQueryBuilder.LateParsingQuery) hasChildQueryBuilder.toQuery(shardContext);
        Similarity expected = SimilarityService.BUILT_IN.get(similarity).apply(similarity, Settings.EMPTY).get();
        assertThat(((PerFieldSimilarityWrapper) query.getSimilarity()).get("custom_string"), instanceOf(expected.getClass()));
    }

    public void testMinFromString() {
        assertThat("fromString(min) != MIN", ScoreMode.Min, equalTo(HasChildQueryBuilder.parseScoreMode("min")));
        assertThat("min", equalTo(HasChildQueryBuilder.scoreModeAsString(ScoreMode.Min)));
    }

    public void testMaxFromString() {
        assertThat("fromString(max) != MAX", ScoreMode.Max, equalTo(HasChildQueryBuilder.parseScoreMode("max")));
        assertThat("max", equalTo(HasChildQueryBuilder.scoreModeAsString(ScoreMode.Max)));
    }

    public void testAvgFromString() {
        assertThat("fromString(avg) != AVG", ScoreMode.Avg, equalTo(HasChildQueryBuilder.parseScoreMode("avg")));
        assertThat("avg", equalTo(HasChildQueryBuilder.scoreModeAsString(ScoreMode.Avg)));
    }

    public void testSumFromString() {
        assertThat("fromString(total) != SUM", ScoreMode.Total, equalTo(HasChildQueryBuilder.parseScoreMode("sum")));
        assertThat("sum", equalTo(HasChildQueryBuilder.scoreModeAsString(ScoreMode.Total)));
    }

    public void testNoneFromString() {
        assertThat("fromString(none) != NONE", ScoreMode.None, equalTo(HasChildQueryBuilder.parseScoreMode("none")));
        assertThat("none", equalTo(HasChildQueryBuilder.scoreModeAsString(ScoreMode.None)));
    }

    /**
     * Should throw {@link IllegalArgumentException} instead of NPE.
     */
    public void testThatNullFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HasChildQueryBuilder.parseScoreMode(null));
        assertEquals("No score mode for child query [null] found", e.getMessage());
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    public void testThatUnrecognizedFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> HasChildQueryBuilder.parseScoreMode("unrecognized value"));
        assertEquals("No score mode for child query [unrecognized value] found", e.getMessage());
    }

    public void testIgnoreUnmapped() throws IOException {
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final HasChildQueryBuilder failingQueryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("[" + HasChildQueryBuilder.NAME + "] no mapping found for type [unmapped]"));
    }
}
