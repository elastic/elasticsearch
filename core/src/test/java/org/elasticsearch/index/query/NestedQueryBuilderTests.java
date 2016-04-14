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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.index.query.support.InnerHitBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.containsString;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MapperService mapperService = queryShardContext().getMapperService();
        mapperService.merge("nested_doc", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("nested_doc",
                STRING_FIELD_NAME, "type=text",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object",
                GEO_POINT_FIELD_NAME, GEO_POINT_FIELD_MAPPING,
                "nested1", "type=nested"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    @Override
    protected void setSearchContext(String[] types) {
        final MapperService mapperService = queryShardContext().getMapperService();
        final IndexFieldDataService fieldData = indexFieldDataService();
        TestSearchContext testSearchContext = new TestSearchContext(queryShardContext()) {

            @Override
            public MapperService mapperService() {
                return mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public IndexFieldDataService fieldData() {
                return fieldData; // need to build / parse inner hits sort fields
            }
        };
        testSearchContext.getQueryShardContext().setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        return new NestedQueryBuilder("nested1", RandomQueryBuilder.createQuery(random()),
                RandomPicks.randomFrom(random(), ScoreMode.values()),
                SearchContext.current() == null ? null : new InnerHitBuilder()
                        .setName(randomAsciiOfLengthBetween(1, 10))
                        .setSize(randomIntBetween(0, 100))
                        .addSort(new FieldSortBuilder(STRING_FIELD_NAME).order(SortOrder.ASC))).ignoreUnmapped(randomBoolean());
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
                assertThat(SearchContext.current().innerHits().getInnerHits().size(), equalTo(0));
            }
        }
    }

    public void testValidate() {
        try {
            new NestedQueryBuilder(null, new MatchAllQueryBuilder());
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

        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder());
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
                "              \"operator\" : \"OR\",\n" +
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
                "    \"ignore_unmapped\" : false,\n" +
                "    \"score_mode\" : \"avg\",\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        NestedQueryBuilder parsed = (NestedQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, ScoreMode.Avg, parsed.scoreMode());
    }

    public void testIgnoreUnmapped() throws IOException {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder());
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(queryShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final NestedQueryBuilder failingQueryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder());
        failingQueryBuilder.ignoreUnmapped(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> failingQueryBuilder.toQuery(queryShardContext()));
        assertThat(e.getMessage(), containsString("[" + NestedQueryBuilder.NAME + "] failed to find nested object under path [unmapped]"));
    }
}
