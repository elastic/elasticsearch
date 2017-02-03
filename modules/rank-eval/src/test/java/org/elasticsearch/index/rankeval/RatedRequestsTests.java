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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class RatedRequestsTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    /**
    * setup for the whole base test class
    */
    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(Stream.of(
                new SearchModule(Settings.EMPTY, false, emptyList()).getNamedXContents().stream()
                ).flatMap(Function.identity()).collect(toList()));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistry = null;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public static RatedRequest createTestItem(List<String> indices, List<String> types, boolean forceRequest) {
        String requestId = randomAsciiOfLength(50);

        List<RatedDocument> ratedDocs = new ArrayList<>();
        int size = randomIntBetween(0, 2);
        for (int i = 0; i < size; i++) {
            ratedDocs.add(RatedDocumentTests.createRatedDocument());
        }

        Map<String, Object> params = new HashMap<>();
        SearchSourceBuilder testRequest = null;
        if (randomBoolean() || forceRequest) {
            testRequest = new SearchSourceBuilder();
            testRequest.size(randomInt());
            testRequest.query(new MatchAllQueryBuilder());
        } else {
            int randomSize = randomIntBetween(1, 10);
            for (int i = 0; i < randomSize; i++) {
                params.put(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            }
        }

        List<String> summaryFields = new ArrayList<>();
        int numSummaryFields = randomIntBetween(0, 5);
        for (int i = 0; i < numSummaryFields; i++) {
            summaryFields.add(randomAsciiOfLength(5));
        }

        RatedRequest ratedRequest = null;
        if (params.size() == 0) {
            ratedRequest = new RatedRequest(requestId, ratedDocs, testRequest);
            ratedRequest.setIndices(indices);
            ratedRequest.setTypes(types);
            ratedRequest.setSummaryFields(summaryFields);
        } else {
            ratedRequest = new RatedRequest(requestId, ratedDocs, params, randomAsciiOfLength(5));
            ratedRequest.setIndices(indices);
            ratedRequest.setTypes(types);
            ratedRequest.setSummaryFields(summaryFields);
        }
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

        RatedRequest testItem = createTestItem(indices, types, randomBoolean());
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();

            RatedRequest parsedItem = RatedRequest.fromXContent(itemParser);
            parsedItem.setIndices(indices); // IRL these come from URL
                                            // parameters - see
                                            // RestRankEvalAction
            parsedItem.setTypes(types); // IRL these come from URL parameters -
                                        // see RestRankEvalAction
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testSerialization() throws IOException {
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

        RatedRequest original = createTestItem(indices, types, randomBoolean());

        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));

        RatedRequest deserialized = RankEvalTestHelper.copy(original, RatedRequest::new, new NamedWriteableRegistry(namedWriteables));
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
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

        RatedRequest testItem = createTestItem(indices, types, randomBoolean());

        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));

        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(testItem),
                RankEvalTestHelper.copy(testItem, RatedRequest::new, new NamedWriteableRegistry(namedWriteables)));
    }

    private RatedRequest mutateTestItem(RatedRequest original) {
        String id = original.getId();
        SearchSourceBuilder testRequest = original.getTestRequest();
        List<RatedDocument> ratedDocs = original.getRatedDocs();
        List<String> indices = original.getIndices();
        List<String> types = original.getTypes();
        Map<String, Object> params = original.getParams();
        List<String> summaryFields = original.getSummaryFields();
        String templateId = original.getTemplateId();

        int mutate = randomIntBetween(0, 5);
        switch (mutate) {
            case 0:
                id = randomValueOtherThan(id, () -> randomAsciiOfLength(10));
                break;
            case 1:
                if (testRequest != null) {
                    int size = randomValueOtherThan(testRequest.size(), () -> randomInt());
                    testRequest = new SearchSourceBuilder();
                    testRequest.size(size);
                    testRequest.query(new MatchAllQueryBuilder());
                } else {
                    if (randomBoolean()) {
                        Map<String, Object> mutated = new HashMap<>();
                        mutated.putAll(params);
                        mutated.put("one_more_key", "one_more_value");
                        params = mutated;
                    } else {
                        templateId = randomValueOtherThan(templateId, () -> randomAsciiOfLength(5));
                    }
                }
                break;
            case 2:
                ratedDocs = Arrays.asList(
                        randomValueOtherThanMany(ratedDocs::contains, () -> RatedDocumentTests.createRatedDocument()));
                break;
            case 3:
                indices = Arrays.asList(randomValueOtherThanMany(indices::contains, () -> randomAsciiOfLength(10)));
                break;
            case 4:
                types =  Arrays.asList(randomValueOtherThanMany(types::contains, () -> randomAsciiOfLength(10)));
                break;
            case 5:
                summaryFields = Arrays.asList(randomValueOtherThanMany(summaryFields::contains, () -> randomAsciiOfLength(10)));
                break;
            default:
                throw new IllegalStateException("Requested to modify more than available parameters.");
        }

        RatedRequest ratedRequest = new RatedRequest(id, ratedDocs, testRequest, params, templateId);
        ratedRequest.setIndices(indices);
        ratedRequest.setTypes(types);
        ratedRequest.setSummaryFields(summaryFields);

        return ratedRequest;
    }

    public void testDuplicateRatedDocThrowsException() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1),
                new RatedDocument(new DocumentKey("index1", "type1", "id1"), 5));

        // search request set, no summary fields
        IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, new SearchSourceBuilder()));
        assertEquals(
                "Found duplicate rated document key [{ \"_index\" : \"index1\", \"_type\" : \"type1\", \"_id\" : \"id1\"}]",
                ex.getMessage());
        // templated path, no summary fields
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        ex = expectThrows(
                IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, params, "templateId"));
        assertEquals(
                "Found duplicate rated document key [{ \"_index\" : \"index1\", \"_type\" : \"type1\", \"_id\" : \"id1\"}]",
                ex.getMessage());
    }

    public void testNullSummaryFieldsTreatment() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        RatedRequest request = new RatedRequest("id", ratedDocs, new SearchSourceBuilder());
        expectThrows(IllegalArgumentException.class, () -> request.setSummaryFields(null));
    }

    public void testNullParamsTreatment() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        RatedRequest request = new RatedRequest("id", ratedDocs, new SearchSourceBuilder(), null, null);
        assertNotNull(request.getParams());
    }

    public void testSettingParamsAndRequestThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, new SearchSourceBuilder(), params, null));
    }

    public void testSettingNeitherParamsNorRequestThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, null, null));
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, null, new HashMap<>(), "templateId"));
    }

    public void testSettingParamsWithoutTemplateIdThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, null, params, null));
    }

    public void testSettingTemplateIdAndRequestThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, new SearchSourceBuilder(), null, "templateId"));
    }

    public void testSettingTemplateIdNoParamsThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, null, null, "templateId"));
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
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, querySpecString)) {
            RatedRequest specification = RatedRequest.fromXContent(parser);
            assertEquals("my_qa_query", specification.getId());
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
}
