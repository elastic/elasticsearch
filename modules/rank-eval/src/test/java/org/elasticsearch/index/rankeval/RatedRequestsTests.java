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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
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
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.Matchers.containsString;

public class RatedRequestsTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(
                Stream.of(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents().stream()).flatMap(Function.identity())
                        .collect(toList()));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistry = null;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public static RatedRequest createTestItem(boolean forceRequest) {
        String requestId = randomAlphaOfLength(50);

        List<RatedDocument> ratedDocs = new ArrayList<>();
        int size = randomIntBetween(0, 2);
        for (int i = 0; i < size; i++) {
            ratedDocs.add(RatedDocumentTests.createRatedDocument());
        }

        Map<String, Object> params = new HashMap<>();
        SearchSourceBuilder testRequest = null;
        if (randomBoolean() || forceRequest) {
            testRequest = new SearchSourceBuilder();
            testRequest.size(randomIntBetween(0, Integer.MAX_VALUE));
            testRequest.query(new MatchAllQueryBuilder());
        } else {
            int randomSize = randomIntBetween(1, 10);
            for (int i = 0; i < randomSize; i++) {
                params.put(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
            }
        }

        List<String> summaryFields = new ArrayList<>();
        int numSummaryFields = randomIntBetween(0, 5);
        for (int i = 0; i < numSummaryFields; i++) {
            summaryFields.add(randomAlphaOfLength(5));
        }

        RatedRequest ratedRequest = null;
        if (params.size() == 0) {
            ratedRequest = new RatedRequest(requestId, ratedDocs, testRequest);
            ratedRequest.addSummaryFields(summaryFields);
        } else {
            ratedRequest = new RatedRequest(requestId, ratedDocs, params, randomAlphaOfLength(5));
            ratedRequest.addSummaryFields(summaryFields);
        }
        return ratedRequest;
    }

    public void testXContentRoundtrip() throws IOException {
        RatedRequest testItem = createTestItem(randomBoolean());
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();

            RatedRequest parsedItem = RatedRequest.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        RatedRequest testItem = createTestItem(randomBoolean());
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            Throwable exception = expectThrows(XContentParseException.class, () -> RatedRequest.fromXContent(parser));
            if (exception.getCause() != null) {
                assertThat(exception.getMessage(), containsString("[request] failed to parse field"));
                exception = exception.getCause();
            }
            assertThat(exception.getMessage(), containsString("unknown field"));
        }
    }

    public void testSerialization() throws IOException {
        RatedRequest original = createTestItem(randomBoolean());
        RatedRequest deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    private static RatedRequest copy(RatedRequest original) throws IOException {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        return ESTestCase.copyWriteable(original, new NamedWriteableRegistry(namedWriteables), RatedRequest::new);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(randomBoolean()), RatedRequestsTests::copy, RatedRequestsTests::mutateTestItem);
    }

    private static RatedRequest mutateTestItem(RatedRequest original) {
        String id = original.getId();
        SearchSourceBuilder evaluationRequest = original.getEvaluationRequest();
        List<RatedDocument> ratedDocs = original.getRatedDocs();
        Map<String, Object> params = original.getParams();
        List<String> summaryFields = original.getSummaryFields();
        String templateId = original.getTemplateId();

        int mutate = randomIntBetween(0, 3);
        switch (mutate) {
        case 0:
            id = randomValueOtherThan(id, () -> randomAlphaOfLength(10));
            break;
        case 1:
            if (evaluationRequest != null) {
                int size = randomValueOtherThan(evaluationRequest.size(), () -> randomInt(Integer.MAX_VALUE));
                evaluationRequest = new SearchSourceBuilder();
                evaluationRequest.size(size);
                evaluationRequest.query(new MatchAllQueryBuilder());
            } else {
                if (randomBoolean()) {
                    Map<String, Object> mutated = new HashMap<>();
                    mutated.putAll(params);
                    mutated.put("one_more_key", "one_more_value");
                    params = mutated;
                } else {
                    templateId = randomValueOtherThan(templateId, () -> randomAlphaOfLength(5));
                }
            }
            break;
        case 2:
            ratedDocs = Arrays.asList(randomValueOtherThanMany(ratedDocs::contains, () -> RatedDocumentTests.createRatedDocument()));
            break;
        case 3:
            summaryFields = Arrays.asList(randomValueOtherThanMany(summaryFields::contains, () -> randomAlphaOfLength(10)));
            break;
        default:
            throw new IllegalStateException("Requested to modify more than available parameters.");
        }

        RatedRequest ratedRequest;
        if (evaluationRequest == null) {
            ratedRequest = new RatedRequest(id, ratedDocs, params, templateId);
        } else {
            ratedRequest = new RatedRequest(id, ratedDocs, evaluationRequest);
        }
        ratedRequest.addSummaryFields(summaryFields);

        return ratedRequest;
    }

    public void testDuplicateRatedDocThrowsException() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1), new RatedDocument("index1", "id1", 5));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("test_query", ratedDocs, new SearchSourceBuilder()));
        assertEquals("Found duplicate rated document key [{\"_index\":\"index1\",\"_id\":\"id1\"}] in evaluation request [test_query]",
                ex.getMessage());
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        ex = expectThrows(IllegalArgumentException.class, () -> new RatedRequest("test_query", ratedDocs, params, "templateId"));
        assertEquals("Found duplicate rated document key [{\"_index\":\"index1\",\"_id\":\"id1\"}] in evaluation request [test_query]",
                ex.getMessage());
    }

    public void testNullSummaryFieldsTreatment() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        RatedRequest request = new RatedRequest("id", ratedDocs, new SearchSourceBuilder());
        expectThrows(NullPointerException.class, () -> request.addSummaryFields(null));
    }

    public void testNullParamsTreatment() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        RatedRequest request = new RatedRequest("id", ratedDocs, new SearchSourceBuilder());
        assertNotNull(request.getParams());
        assertEquals(0, request.getParams().size());
    }

    public void testSettingNeitherParamsNorRequestThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, null, null));
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, new HashMap<>(), "templateId"));
    }

    public void testSettingParamsWithoutTemplateIdThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, params, null));
    }

    public void testSettingTemplateIdNoParamsThrows() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, null, "templateId"));
    }

    public void testAggsNotAllowed() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        SearchSourceBuilder query = new SearchSourceBuilder();
        query.aggregation(AggregationBuilders.terms("fieldName"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, query));
        assertEquals("Query in rated requests should not contain aggregations.", e.getMessage());
    }

    public void testSuggestionsNotAllowed() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        SearchSourceBuilder query = new SearchSourceBuilder();
        query.suggest(new SuggestBuilder().addSuggestion("id", SuggestBuilders.completionSuggestion("fieldname")));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, query));
        assertEquals("Query in rated requests should not contain a suggest section.", e.getMessage());
    }

    public void testHighlighterNotAllowed() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        SearchSourceBuilder query = new SearchSourceBuilder();
        query.highlighter(new HighlightBuilder().field("field"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RatedRequest("id", ratedDocs, query));
        assertEquals("Query in rated requests should not contain a highlighter section.", e.getMessage());
    }

    public void testExplainNotAllowed() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, new SearchSourceBuilder().explain(true)));
        assertEquals("Query in rated requests should not use explain.", e.getMessage());
    }

    public void testProfileNotAllowed() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new RatedRequest("id", ratedDocs, new SearchSourceBuilder().profile(true)));
        assertEquals("Query in rated requests should not use profile.", e.getMessage());
    }

    /**
     * test that modifying the order of index/docId to make sure it doesn't
     * matter for parsing xContent
     */
    public void testParseFromXContent() throws IOException {
        String querySpecString = " {\n"
                + "   \"id\": \"my_qa_query\",\n"
                + "   \"request\": {\n"
                + "           \"query\": {\n"
                + "               \"bool\": {\n"
                + "                   \"must\": [\n"
                + "                       {\"match\": {\"beverage\": \"coffee\"}},\n"
                + "                       {\"term\": {\"browser\": {\"value\": \"safari\"}}},\n"
                + "                       {\"term\": {\"time_of_day\": "
                + "                                  {\"value\": \"morning\",\"boost\": 2}}},\n"
                + "                       {\"term\": {\"ip_location\": "
                + "                                  {\"value\": \"ams\",\"boost\": 10}}}]}\n"
                + "           },\n"
                + "           \"size\": 10\n"
                + "   },\n"
                + "   \"summary_fields\" : [\"title\"],\n"
                + "   \"ratings\": [\n"
                + "        {\"_index\": \"test\" , \"_id\": \"1\", \"rating\" : 1 },\n"
                + "        {\"_index\": \"test\", \"rating\" : 0, \"_id\": \"2\"},\n"
                + "        {\"_id\": \"3\", \"_index\": \"test\", \"rating\" : 1} ]"
                + "}\n";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, querySpecString)) {
            RatedRequest specification = RatedRequest.fromXContent(parser);
            assertEquals("my_qa_query", specification.getId());
            assertNotNull(specification.getEvaluationRequest());
            List<RatedDocument> ratedDocs = specification.getRatedDocs();
            assertEquals(3, ratedDocs.size());
            for (int i = 0; i < 3; i++) {
                assertEquals("" + (i + 1), ratedDocs.get(i).getDocID());
                assertEquals("test", ratedDocs.get(i).getIndex());
                if (i == 1) {
                    assertEquals(0, ratedDocs.get(i).getRating());
                } else {
                    assertEquals(1, ratedDocs.get(i).getRating());
                }
            }
        }
    }
}
