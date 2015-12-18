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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

public class HasChildQueryBuilderTests extends AbstractQueryTestCase<HasChildQueryBuilder> {
    protected static final String PARENT_TYPE = "parent";
    protected static final String CHILD_TYPE = "child";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MapperService mapperService = queryShardContext().getMapperService();
        mapperService.merge(PARENT_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(PARENT_TYPE,
                STRING_FIELD_NAME, "type=string",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), false, false);
        mapperService.merge(CHILD_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(CHILD_TYPE,
                "_parent", "type=" + PARENT_TYPE,
                STRING_FIELD_NAME, "type=string",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), false, false);
    }

    @Override
    protected void setSearchContext(String[] types) {
        final MapperService mapperService = queryShardContext().getMapperService();
        final IndexFieldDataService fieldData = indexFieldDataService();
        TestSearchContext testSearchContext = new TestSearchContext() {
            private InnerHitsContext context;


            @Override
            public void innerHits(InnerHitsContext innerHitsContext) {
                context = innerHitsContext;
            }

            @Override
            public InnerHitsContext innerHits() {
                return context;
            }

            @Override
            public MapperService mapperService() {
                return mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public IndexFieldDataService fieldData() {
                return fieldData; // need to build / parse inner hits sort fields
            }
        };
        testSearchContext.setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasChildQueryBuilder doCreateTestQueryBuilder() {
        int min = randomIntBetween(0, Integer.MAX_VALUE / 2);
        int max = randomIntBetween(min, Integer.MAX_VALUE);
        InnerHitsBuilder.InnerHit innerHit = new InnerHitsBuilder.InnerHit().setSize(100).addSort(STRING_FIELD_NAME, SortOrder.ASC);
        return new HasChildQueryBuilder(CHILD_TYPE,
                RandomQueryBuilder.createQuery(random()), max, min,
                RandomPicks.randomFrom(random(), ScoreMode.values()),
                randomBoolean()  ? null : new QueryInnerHits("inner_hits_name", innerHit));
    }

    @Override
    protected void doAssertLuceneQuery(HasChildQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQueryBuilder = queryBuilder.query();
        if (innerQueryBuilder instanceof EmptyQueryBuilder) {
            assertNull(query);
        } else {
            assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
            HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
            assertEquals(queryBuilder.minChildren(), lpq.getMinChildren());
            assertEquals(queryBuilder.maxChildren(), lpq.getMaxChildren());
            assertEquals(queryBuilder.scoreMode(), lpq.getScoreMode()); // WTF is this why do we have two?
        }
        if (queryBuilder.innerHit() != null) {
            assertNotNull(SearchContext.current());
            if (query != null) {
                assertNotNull(SearchContext.current().innerHits());
                assertEquals(1, SearchContext.current().innerHits().getInnerHits().size());
                assertTrue(SearchContext.current().innerHits().getInnerHits().containsKey("inner_hits_name"));
                InnerHitsContext.BaseInnerHits innerHits = SearchContext.current().innerHits().getInnerHits().get("inner_hits_name");
                assertEquals(innerHits.size(), 100);
                assertEquals(innerHits.sort().getSort().length, 1);
                assertEquals(innerHits.sort().getSort()[0].getField(), STRING_FIELD_NAME);
            } else {
                assertNull(SearchContext.current().innerHits());
            }
        }
    }

    public void testIllegalValues() {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());
        try {
            new HasChildQueryBuilder(null, query);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }

        try {
            new HasChildQueryBuilder("foo", null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        HasChildQueryBuilder foo = new HasChildQueryBuilder("foo", query);// all good
        try {
            foo.scoreMode(null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        final int positiveValue = randomIntBetween(0, Integer.MAX_VALUE);
        try {
            foo.minChildren(randomIntBetween(Integer.MIN_VALUE, -1));
            fail("must not be negative");
        } catch (IllegalArgumentException ex) {

        }
        foo.minChildren(positiveValue);
        assertEquals(positiveValue, foo.minChildren());
        try {
            foo.maxChildren(randomIntBetween(Integer.MIN_VALUE, -1));
            fail("must not be negative");
        } catch (IllegalArgumentException ex) {

        }

        foo.maxChildren(positiveValue);
        assertEquals(positiveValue, foo.maxChildren());
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
                "    \"boost\" : 2.0,\n" +
                "    \"_name\" : \"WNzYMJKRwePuRBh\",\n" +
                "    \"inner_hits\" : {\n" +
                "      \"name\" : \"inner_hits_name\",\n" +
                "      \"size\" : 100,\n" +
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
        assertEquals(query, queryBuilder.innerHit(), new QueryInnerHits("inner_hits_name", new InnerHitsBuilder.InnerHit().setSize(100).addSort("mapped_string", SortOrder.ASC)));

    }
    public void testToQueryInnerQueryType() throws IOException {
        String[] searchTypes = new String[]{PARENT_TYPE};
        QueryShardContext.setTypes(searchTypes);
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(CHILD_TYPE, new IdsQueryBuilder().addIds("id"));
        Query query = hasChildQueryBuilder.toQuery(createShardContext());
        //verify that the context types are still the same as the ones we previously set
        assertThat(QueryShardContext.getTypes(), equalTo(searchTypes));
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
        assertThat(booleanTermsQuery.clauses().size(), equalTo(1));
        assertThat(booleanTermsQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanTermsQuery.clauses().get(0).getQuery(), instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) booleanTermsQuery.clauses().get(0).getQuery();
        assertThat(termQuery.getTerm().field(), equalTo(UidFieldMapper.NAME));
        //we want to make sure that the inner ids query gets executed against the child type rather than the main type we initially set to the context
        BytesRef[] ids = Uid.createUidsForTypesAndIds(Collections.singletonList(type), Collections.singletonList(id));
        assertThat(termQuery.getTerm().bytes(), equalTo(ids[0]));
        //check the type filter
        assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.FILTER));
        assertThat(booleanQuery.clauses().get(1).getQuery(), instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery typeConstantScoreQuery = (ConstantScoreQuery) booleanQuery.clauses().get(1).getQuery();
        assertThat(typeConstantScoreQuery.getQuery(), instanceOf(TermQuery.class));
        TermQuery typeTermQuery = (TermQuery) typeConstantScoreQuery.getQuery();
        assertThat(typeTermQuery.getTerm().field(), equalTo(TypeFieldMapper.NAME));
        assertThat(typeTermQuery.getTerm().text(), equalTo(type));
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
                } catch (ParsingException | ScriptParseException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
    }
}
