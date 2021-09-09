/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.rankeval.RankEvalSpec.ScriptWithId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.Matchers.containsString;

public class RankEvalSpecTests extends ESTestCase {

    @SuppressWarnings("resource")
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new RankEvalPlugin().getNamedXContent());
    }

    private static <T> List<T> randomList(Supplier<T> randomSupplier) {
        List<T> result = new ArrayList<>();
        int size = randomIntBetween(1, 20);
        for (int i = 0; i < size; i++) {
            result.add(randomSupplier.get());
        }
        return result;
    }

    static RankEvalSpec createTestItem() {
        Supplier<EvaluationMetric> metric = randomFrom(Arrays.asList(
                () -> PrecisionAtKTests.createTestItem(),
                () -> RecallAtKTests.createTestItem(),
                () -> MeanReciprocalRankTests.createTestItem(),
                () -> DiscountedCumulativeGainTests.createTestItem()));

        List<RatedRequest> ratedRequests = null;
        Collection<ScriptWithId> templates = null;

        if (randomBoolean()) {
            final Map<String, Object> params = randomBoolean() ? Collections.emptyMap() : Collections.singletonMap("key", "value");
            String script;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("field", randomAlphaOfLengthBetween(1, 5));
                builder.endObject();
                script = Strings.toString(builder);
            } catch (IOException e) {
                // this shouldn't happen in tests, re-throw just not to swallow it
                throw new RuntimeException(e);
            }

            templates = new HashSet<>();
            templates.add(new ScriptWithId("templateId", new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, script, params)));

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("key", "value");
            RatedRequest ratedRequest = new RatedRequest("id", Arrays.asList(RatedDocumentTests.createRatedDocument()), templateParams,
                    "templateId");
            ratedRequests = Arrays.asList(ratedRequest);
        } else {
            RatedRequest ratedRequest = new RatedRequest("id", Arrays.asList(RatedDocumentTests.createRatedDocument()),
                    new SearchSourceBuilder());
            ratedRequests = Arrays.asList(ratedRequest);
        }
        RankEvalSpec spec = new RankEvalSpec(ratedRequests, metric.get(), templates);
        maybeSet(spec::setMaxConcurrentSearches, randomInt(100));
        List<String> indices = new ArrayList<>();
        int size = randomIntBetween(0, 20);
        for (int i = 0; i < size; i++) {
            indices.add(randomAlphaOfLengthBetween(0, 50));
        }
        return spec;
    }

    public void testXContentRoundtrip() throws IOException {
        RankEvalSpec testItem = createTestItem();
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(shuffled))) {
            RankEvalSpec parsedItem = RankEvalSpec.parse(parser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        RankEvalSpec testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            Exception exception = expectThrows(Exception.class, () -> RankEvalSpec.parse(parser));
            assertThat(exception.getMessage(), containsString("[rank_eval] failed to parse field"));
        }
    }

    public void testSerialization() throws IOException {
        RankEvalSpec original = createTestItem();
        RankEvalSpec deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    private static RankEvalSpec copy(RankEvalSpec original) throws IOException {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, PrecisionAtK.NAME, PrecisionAtK::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, RecallAtK.NAME, RecallAtK::new));
        namedWriteables.add(
                new NamedWriteableRegistry.Entry(EvaluationMetric.class, DiscountedCumulativeGain.NAME, DiscountedCumulativeGain::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, MeanReciprocalRank.NAME, MeanReciprocalRank::new));
        return ESTestCase.copyWriteable(original, new NamedWriteableRegistry(namedWriteables), RankEvalSpec::new);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), RankEvalSpecTests::copy, RankEvalSpecTests::mutateTestItem);
    }

    static RankEvalSpec mutateTestItem(RankEvalSpec original) {
        List<RatedRequest> ratedRequests = new ArrayList<>(original.getRatedRequests());
        EvaluationMetric metric = original.getMetric();
        Map<String, Script> templates = new HashMap<>(original.getTemplates());

        int mutate = randomIntBetween(0, 2);
        switch (mutate) {
        case 0:
            RatedRequest request = RatedRequestsTests.createTestItem(true);
            ratedRequests.add(request);
            break;
        case 1:
            if (metric instanceof PrecisionAtK) {
                metric = new DiscountedCumulativeGain();
            } else {
                metric = new PrecisionAtK();
            }
            break;
        case 2:
            templates.put("mutation", new Script(ScriptType.INLINE, "mustache", randomAlphaOfLength(10), new HashMap<>()));
            break;
        default:
            throw new IllegalStateException("Requested to modify more than available parameters.");
        }

        List<ScriptWithId> scripts = new ArrayList<>();
        for (Entry<String, Script> entry : templates.entrySet()) {
            scripts.add(new ScriptWithId(entry.getKey(), entry.getValue()));
        }
        RankEvalSpec result = new RankEvalSpec(ratedRequests, metric, scripts);
        return result;
    }

    public void testMissingRatedRequestsFails() {
        EvaluationMetric metric = new PrecisionAtK();
        expectThrows(IllegalArgumentException.class, () -> new RankEvalSpec(new ArrayList<>(), metric));
        expectThrows(IllegalArgumentException.class, () -> new RankEvalSpec(null, metric));
    }

    public void testMissingMetricFails() {
        List<RatedRequest> ratedRequests = randomList(() -> RatedRequestsTests.createTestItem(randomBoolean()));
        expectThrows(NullPointerException.class, () -> new RankEvalSpec(ratedRequests, null));
    }

    public void testMissingTemplateAndSearchRequestFails() {
        List<RatedDocument> ratedDocs = Arrays.asList(new RatedDocument("index1", "id1", 1));
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        RatedRequest request = new RatedRequest("id", ratedDocs, params, "templateId");
        List<RatedRequest> ratedRequests = Arrays.asList(request);
        expectThrows(IllegalStateException.class, () -> new RankEvalSpec(ratedRequests, new PrecisionAtK()));
    }
}
