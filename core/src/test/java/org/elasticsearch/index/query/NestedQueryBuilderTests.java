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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MapperService mapperService = queryShardContext().getMapperService();
        mapperService.merge("nested_doc", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("nested_doc",
                STRING_FIELD_NAME, "type=string",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object",
                "nested1", "type=nested"
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
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        InnerHitsBuilder.InnerHit innerHit = new InnerHitsBuilder.InnerHit().setSize(100).addSort(STRING_FIELD_NAME, SortOrder.ASC);
        return new NestedQueryBuilder("nested1", RandomQueryBuilder.createQuery(random()),
                RandomPicks.randomFrom(random(), ScoreMode.values()),
                SearchContext.current() == null ? null : new QueryInnerHits("inner_hits_name", innerHit));
    }

    @Override
    protected void doAssertLuceneQuery(NestedQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQueryBuilder = queryBuilder.query();
        if (innerQueryBuilder instanceof EmptyQueryBuilder) {
            assertNull(query);
        } else {
            assertThat(query, instanceOf(ToParentBlockJoinQuery.class));
            ToParentBlockJoinQuery parentBlockJoinQuery = (ToParentBlockJoinQuery) query;
            //TODO how to assert this?
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

    public void testValidate() {
        try {
            new NestedQueryBuilder(null, EmptyQueryBuilder.PROTOTYPE);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new NestedQueryBuilder("path", null);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }

        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", EmptyQueryBuilder.PROTOTYPE);
        try {
            nestedQueryBuilder.scoreMode(null);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" + 
                "  \"nested\" : {\n" + 
                "    \"query\" : {\n" + 
                "      \"bool\" : {\n" + 
                "        \"must\" : [ {\n" + 
                "          \"match\" : {\n" + 
                "            \"obj1.name\" : {\n" + 
                "              \"query\" : \"blue\",\n" + 
                "              \"type\" : \"boolean\",\n" + 
                "              \"operator\" : \"OR\",\n" + 
                "              \"slop\" : 0,\n" + 
                "              \"prefix_length\" : 0,\n" + 
                "              \"max_expansions\" : 50,\n" + 
                "              \"fuzzy_transpositions\" : true,\n" + 
                "              \"lenient\" : false,\n" + 
                "              \"zero_terms_query\" : \"NONE\",\n" + 
                "              \"boost\" : 1.0\n" + 
                "            }\n" + 
                "          }\n" + 
                "        }, {\n" + 
                "          \"range\" : {\n" + 
                "            \"obj1.count\" : {\n" + 
                "              \"from\" : 5,\n" + 
                "              \"to\" : null,\n" + 
                "              \"include_lower\" : false,\n" + 
                "              \"include_upper\" : true,\n" + 
                "              \"boost\" : 1.0\n" + 
                "            }\n" + 
                "          }\n" + 
                "        } ],\n" + 
                "        \"disable_coord\" : false,\n" + 
                "        \"adjust_pure_negative\" : true,\n" + 
                "        \"boost\" : 1.0\n" + 
                "      }\n" + 
                "    },\n" + 
                "    \"path\" : \"obj1\",\n" + 
                "    \"score_mode\" : \"avg\",\n" + 
                "    \"boost\" : 1.0\n" + 
                "  }\n" + 
                "}";

        NestedQueryBuilder parsed = (NestedQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, ScoreMode.Avg, parsed.scoreMode());
    }
}
