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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

public class HasParentQueryBuilderTests extends AbstractQueryTestCase<HasParentQueryBuilder> {
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
    protected HasParentQueryBuilder doCreateTestQueryBuilder() {
        InnerHitsBuilder.InnerHit innerHit = new InnerHitsBuilder.InnerHit().setSize(100).addSort(STRING_FIELD_NAME, SortOrder.ASC);
        return new HasParentQueryBuilder(PARENT_TYPE,
                RandomQueryBuilder.createQuery(random()),randomBoolean(),
                randomBoolean() ? null : new QueryInnerHits("inner_hits_name", innerHit));
    }

    @Override
    protected void doAssertLuceneQuery(HasParentQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQueryBuilder = queryBuilder.query();
        if (innerQueryBuilder instanceof EmptyQueryBuilder) {
            assertNull(query);
        } else {
            assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
            HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
            assertEquals(queryBuilder.score() ? ScoreMode.Max : ScoreMode.None, lpq.getScoreMode());
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
            new HasParentQueryBuilder(null, query);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }

        try {
            new HasParentQueryBuilder("foo", null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
    }

    public void testDeprecatedXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.startObject("has_parent");
        builder.field("query");
        EmptyQueryBuilder.PROTOTYPE.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field("type", "foo"); // deprecated
        builder.endObject();
        builder.endObject();
        try {
            parseQuery(builder.string());
            fail("type is deprecated");
        } catch (IllegalArgumentException ex) {
            assertEquals("Deprecated field [type] used, expected [parent_type] instead", ex.getMessage());
        }

        HasParentQueryBuilder queryBuilder = (HasParentQueryBuilder) parseQuery(builder.string(), ParseFieldMatcher.EMPTY);
        assertEquals("foo", queryBuilder.type());

        boolean score = randomBoolean();
        String key = RandomPicks.randomFrom(random(), Arrays.asList("score_mode", "scoreMode"));
        builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.startObject("has_parent");
        builder.field("query");
        EmptyQueryBuilder.PROTOTYPE.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field(key, score ? "score": "none");
        builder.field("parent_type", "foo");
        builder.endObject();
        builder.endObject();
        try {
            parseQuery(builder.string());
            fail(key + " is deprecated");
        } catch (IllegalArgumentException ex) {
            assertEquals("Deprecated field [" + key + "] used, replaced by [score]", ex.getMessage());
        }

        queryBuilder = (HasParentQueryBuilder) parseQuery(builder.string(), ParseFieldMatcher.EMPTY);
        assertEquals(score, queryBuilder.score());
    }

    public void testToQueryInnerQueryType() throws IOException {
        String[] searchTypes = new String[]{CHILD_TYPE};
        QueryShardContext.setTypes(searchTypes);
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(PARENT_TYPE, new IdsQueryBuilder().addIds("id"));
        Query query = hasParentQueryBuilder.toQuery(createShardContext());
        //verify that the context types are still the same as the ones we previously set
        assertThat(QueryShardContext.getTypes(), equalTo(searchTypes));
        HasChildQueryBuilderTests.assertLateParsingQuery(query, PARENT_TYPE, "id");
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

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"has_parent\" : {\n" +
                "    \"query\" : {\n" +
                "      \"term\" : {\n" +
                "        \"tag\" : {\n" +
                "          \"value\" : \"something\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"parent_type\" : \"blog\",\n" +
                "    \"score\" : true,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        HasParentQueryBuilder parsed = (HasParentQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "blog", parsed.type());
        assertEquals(json, "something", ((TermQueryBuilder) parsed.query()).value());
    }
}
