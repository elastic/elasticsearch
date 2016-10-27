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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class RatedRequestsTests extends ESTestCase {

    private static SearchModule searchModule;
    private static SearchRequestParsers searchRequestParsers;

    /**
    * setup for the whole base test class
    */
    @BeforeClass
    public static void init() throws IOException {
        AggregatorParsers aggsParsers = new AggregatorParsers(new ParseFieldRegistry<>("aggregation"),
                new ParseFieldRegistry<>("aggregation_pipes"));
        searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        IndicesQueriesRegistry queriesRegistry = searchModule.getQueryParserRegistry();
        Suggesters suggesters = searchModule.getSuggesters();
        searchRequestParsers = new SearchRequestParsers(queriesRegistry, aggsParsers, suggesters, null);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        searchModule = null;
        searchRequestParsers = null;
    }

    public static RatedRequest createTestItem(List<String> indices, List<String> types) {
        String specId = randomAsciiOfLength(50);

        SearchSourceBuilder testRequest = new SearchSourceBuilder();
        testRequest.size(randomInt());
        testRequest.query(new MatchAllQueryBuilder());

        List<RatedDocument> ratedDocs = new ArrayList<>();
        int size = randomIntBetween(0, 2);
        for (int i = 0; i < size; i++) {
            ratedDocs.add(RatedDocumentTests.createRatedDocument());
        }

        RatedRequest ratedRequest = new RatedRequest(specId, testRequest, indices, types, ratedDocs);


        if (randomBoolean()) {
            Map<String, String> params = new HashMap<String, String>();
            int randomSize = randomIntBetween(1, 10);
            for (int i = 0; i < randomSize; i++) {
                params.put(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            }
            ratedRequest.setParams(params);
        }

        List<String> summaryFields = new ArrayList<>();
        int numSummaryFields = randomIntBetween(0, 5);
        for (int i = 0; i < numSummaryFields; i++) {
            summaryFields.add(randomAsciiOfLength(5));
        }
        ratedRequest.setSummaryFields(summaryFields);
        return ratedRequest;
    }

    public void testXContentRoundtrip() throws IOException {
        List<String> indices = new ArrayList<>();
        int size = randomIntBetween(0, 20);
        for (int i = 0; i < size; i++) {
            indices.add(randomAsciiOfLengthBetween(0, 50));
        }

        List<String> types = new ArrayList<>();
        size = randomIntBetween(0, 20);
        for (int i = 0; i < size; i++) {
            types.add(randomAsciiOfLengthBetween(0, 50));
        }

        RatedRequest testItem = createTestItem(indices, types);
        XContentParser itemParser = RankEvalTestHelper.roundtrip(testItem);
        itemParser.nextToken();

        QueryParseContext queryContext = new QueryParseContext(searchRequestParsers.queryParsers, itemParser, ParseFieldMatcher.STRICT);
        RankEvalContext rankContext = new RankEvalContext(ParseFieldMatcher.STRICT, queryContext,
                searchRequestParsers, null);

        RatedRequest parsedItem = RatedRequest.fromXContent(itemParser, rankContext);
        parsedItem.setIndices(indices); // IRL these come from URL parameters - see RestRankEvalAction
        parsedItem.setTypes(types); // IRL these come from URL parameters - see RestRankEvalAction
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());
    }

    public void testDuplicateRatedDocThrowsException() {
        RatedRequest request = createTestItem(Arrays.asList("index"), Arrays.asList("type"));
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1),
                new RatedDocument(new DocumentKey("index1", "type1", "id1"), 5));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> request.setRatedDocs(ratedDocs));
        assertEquals(
                "Found duplicate rated document key [{ \"_index\" : \"index1\", \"_type\" : \"type1\", \"_id\" : \"id1\"}]",
                ex.getMessage());
    }

    public void testParseFromXContent() throws IOException {
        // we modify the order of index/type/docId to make sure it doesn't matter for parsing xContent
        String querySpecString = " {\n"
         + "   \"id\": \"my_qa_query\",\n"
         + "   \"request\": {\n"
         + "           \"query\": {\n"
         + "               \"bool\": {\n"
         + "                   \"must\": [\n"
         + "                       {\"match\": {\"beverage\": \"coffee\"}},\n"
         + "                       {\"term\": {\"browser\": {\"value\": \"safari\"}}},\n"
         + "                       {\"term\": {\"time_of_day\": {\"value\": \"morning\",\"boost\": 2}}},\n"
         + "                       {\"term\": {\"ip_location\": {\"value\": \"ams\",\"boost\": 10}}}]}\n"
         + "           },\n"
         + "           \"size\": 10\n"
         + "   },\n"
         + "   \"summary_fields\" : [\"title\"],\n"
         + "   \"ratings\": [ "
         + "        {\"_index\": \"test\", \"_type\": \"testtype\", \"_id\": \"1\", \"rating\" : 1 }, "
         + "        {\"_type\": \"testtype\", \"_index\": \"test\", \"_id\": \"2\", \"rating\" : 0 }, "
         + "        {\"_id\": \"3\", \"_index\": \"test\", \"_type\": \"testtype\", \"rating\" : 1 }]\n"
         + "}";
        XContentParser parser = XContentFactory.xContent(querySpecString).createParser(querySpecString);
        QueryParseContext queryContext = new QueryParseContext(searchRequestParsers.queryParsers, parser, ParseFieldMatcher.STRICT);
        RankEvalContext rankContext = new RankEvalContext(ParseFieldMatcher.STRICT, queryContext,
                searchRequestParsers, null);
        RatedRequest specification = RatedRequest.fromXContent(parser, rankContext);
        assertEquals("my_qa_query", specification.getSpecId());
        assertNotNull(specification.getTestRequest());
        List<RatedDocument> ratedDocs = specification.getRatedDocs();
        assertEquals(3, ratedDocs.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("" + (i + 1), ratedDocs.get(i).getDocID());
            assertEquals("test", ratedDocs.get(i).getIndex());
            assertEquals("testtype", ratedDocs.get(i).getType());
            if (i == 1) {
                assertEquals(0, ratedDocs.get(i).getRating());
            } else {
                assertEquals(1, ratedDocs.get(i).getRating());
            }
        }
    }
}
