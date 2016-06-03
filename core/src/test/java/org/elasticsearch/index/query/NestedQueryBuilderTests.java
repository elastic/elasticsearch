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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
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

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        NestedQueryBuilder nqb = new NestedQueryBuilder("nested1", RandomQueryBuilder.createQuery(random()),
                RandomPicks.randomFrom(random(), ScoreMode.values()));
        if (randomBoolean()) {
            nqb.innerHit(new InnerHitBuilder()
                    .setName(randomAsciiOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(INT_FIELD_NAME).order(SortOrder.ASC)));
        }
        nqb.ignoreUnmapped(randomBoolean());
        return nqb;
    }

    @Override
    protected void doAssertLuceneQuery(NestedQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQueryBuilder = queryBuilder.query();
        assertThat(query, instanceOf(ToParentBlockJoinQuery.class));
        ToParentBlockJoinQuery parentBlockJoinQuery = (ToParentBlockJoinQuery) query;
        // TODO how to assert this?
        if (queryBuilder.innerHit() != null) {
            SearchContext searchContext = SearchContext.current();
            assertNotNull(searchContext);
            if (query != null) {
                Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
                InnerHitBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
                for (InnerHitBuilder builder : innerHitBuilders.values()) {
                    builder.build(searchContext, searchContext.innerHits());
                }
                assertNotNull(searchContext.innerHits());
                assertEquals(1, searchContext.innerHits().getInnerHits().size());
                assertTrue(searchContext.innerHits().getInnerHits().containsKey(queryBuilder.innerHit().getName()));
                InnerHitsContext.BaseInnerHits innerHits = searchContext.innerHits().getInnerHits().get(queryBuilder.innerHit().getName());
                assertEquals(innerHits.size(), queryBuilder.innerHit().getSize());
                assertEquals(innerHits.sort().sort.getSort().length, 1);
                assertEquals(innerHits.sort().sort.getSort()[0].getField(), INT_FIELD_NAME);
            } else {
                assertThat(searchContext.innerHits().getInnerHits().size(), equalTo(0));
            }
        }
    }

    public void testValidate() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery(null, innerQuery, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'path' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", null, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'query' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", innerQuery, null));
        assertThat(e.getMessage(), equalTo("[nested] requires 'score_mode' field"));
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
                String testQuery = validQuery.substring(0, insertionPosition) + "{ \"newField\" : " +
                        validQuery.substring(insertionPosition) + "}";
                try {
                    parseQuery(testQuery);
                    fail("some parsing exception expected for query: " + testQuery);
                } catch (ParsingException | Script.ScriptParseException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
    }

    public void testIgnoreUnmapped() throws IOException {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final NestedQueryBuilder failingQueryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.ignoreUnmapped(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("[" + NestedQueryBuilder.NAME + "] failed to find nested object under path [unmapped]"));
    }
}
