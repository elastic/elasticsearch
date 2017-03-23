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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
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

public class RankEvalSpecTests extends ESTestCase {

    private static <T> List<T> randomList(Supplier<T> randomSupplier) {
        List<T> result = new ArrayList<>();
        int size = randomIntBetween(1, 20);
        for (int i = 0; i < size; i++) {
            result.add(randomSupplier.get());
        }
        return result;
    }

    private static RankEvalSpec createTestItem() throws IOException {
        RankedListQualityMetric metric;
        if (randomBoolean()) {
            metric = PrecisionTests.createTestItem();
        } else {
            metric = DiscountedCumulativeGainTests.createTestItem();
        }

        List<RatedRequest> ratedRequests = null;
        Collection<ScriptWithId> templates = null;

        if (randomBoolean()) {
            final Map<String, Object> params = randomBoolean() ? Collections.emptyMap()
                    : Collections.singletonMap("key", "value");
            ScriptType scriptType = randomFrom(ScriptType.values());
            String script;
            if (scriptType == ScriptType.INLINE) {
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    builder.startObject();
                    builder.field("field", randomAsciiOfLengthBetween(1, 5));
                    builder.endObject();
                    script = builder.string();
                }
            } else {
                script = randomAsciiOfLengthBetween(1, 5);
            }

            templates = new HashSet<>();
            templates.add(new ScriptWithId("templateId",
                    new Script(scriptType, Script.DEFAULT_TEMPLATE_LANG, script, params)));

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("key", "value");
            RatedRequest ratedRequest = new RatedRequest("id",
                    Arrays.asList(RatedDocumentTests.createRatedDocument()), templateParams,
                    "templateId");
            ratedRequests = Arrays.asList(ratedRequest);
        } else {
            RatedRequest ratedRequest = new RatedRequest("id",
                    Arrays.asList(RatedDocumentTests.createRatedDocument()),
                    new SearchSourceBuilder());
            ratedRequests = Arrays.asList(ratedRequest);
        }
        RankEvalSpec spec = new RankEvalSpec(ratedRequests, metric, templates);
        maybeSet(spec::setMaxConcurrentSearches, randomInt(100));
        return spec;
    }

    public void testXContentRoundtrip() throws IOException {
        RankEvalSpec testItem = createTestItem();

        XContentBuilder shuffled = shuffleXContent(
                testItem.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, shuffled.bytes())) {

            RankEvalSpec parsedItem = RankEvalSpec.parse(parser);
            // IRL these come from URL parameters - see RestRankEvalAction
            // TODO Do we still need this?
            // parsedItem.getRatedRequests().stream().forEach(e ->
            // {e.setIndices(indices); e.setTypes(types);});
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testSerialization() throws IOException {
        RankEvalSpec original = createTestItem();

        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class,
                MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                Precision.NAME, Precision::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                DiscountedCumulativeGain.NAME, DiscountedCumulativeGain::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                ReciprocalRank.NAME, ReciprocalRank::new));

        RankEvalSpec deserialized = RankEvalTestHelper.copy(original, RankEvalSpec::new,
                new NamedWriteableRegistry(namedWriteables));
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        RankEvalSpec testItem = createTestItem();

        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class,
                MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                Precision.NAME, Precision::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                DiscountedCumulativeGain.NAME, DiscountedCumulativeGain::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankedListQualityMetric.class,
                ReciprocalRank.NAME, ReciprocalRank::new));

        RankEvalSpec mutant = RankEvalTestHelper.copy(testItem, RankEvalSpec::new,
                new NamedWriteableRegistry(namedWriteables));
        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(mutant),
                RankEvalTestHelper.copy(testItem, RankEvalSpec::new,
                        new NamedWriteableRegistry(namedWriteables)));
    }

    private static RankEvalSpec mutateTestItem(RankEvalSpec mutant) {
        Collection<RatedRequest> ratedRequests = mutant.getRatedRequests();
        RankedListQualityMetric metric = mutant.getMetric();
        Map<String, Script> templates = mutant.getTemplates();

        int mutate = randomIntBetween(0, 2);
        switch (mutate) {
        case 0:
            RatedRequest request = RatedRequestsTests.createTestItem(new ArrayList<>(),
                    new ArrayList<>(), true);
            ratedRequests.add(request);
            break;
        case 1:
            if (metric instanceof Precision) {
                metric = new DiscountedCumulativeGain();
            } else {
                metric = new Precision();
            }
            break;
        case 2:
            if (templates.size() > 0) {
                String mutatedTemplate = randomAsciiOfLength(10);
                templates.put("mutation", new Script(ScriptType.INLINE, "mustache", mutatedTemplate,
                        new HashMap<>()));
            } else {
                String mutatedTemplate = randomValueOtherThanMany(templates::containsValue,
                        () -> randomAsciiOfLength(10));
                templates.put("mutation", new Script(ScriptType.INLINE, "mustache", mutatedTemplate,
                        new HashMap<>()));
            }
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

    public void testMissingRatedRequestsFailsParsing() {
        RankedListQualityMetric metric = new Precision();
        expectThrows(IllegalStateException.class,
                () -> new RankEvalSpec(new ArrayList<>(), metric));
        expectThrows(IllegalStateException.class, () -> new RankEvalSpec(null, metric));
    }

    public void testMissingMetricFailsParsing() {
        List<String> strings = Arrays.asList("value");
        List<RatedRequest> ratedRequests = randomList(
                () -> RatedRequestsTests.createTestItem(strings, strings, randomBoolean()));
        expectThrows(IllegalStateException.class, () -> new RankEvalSpec(ratedRequests, null));
    }

    public void testMissingTemplateAndSearchRequestFailsParsing() {
        List<RatedDocument> ratedDocs = Arrays
                .asList(new RatedDocument(new DocumentKey("index1", "type1", "id1"), 1));
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");

        RatedRequest request = new RatedRequest("id", ratedDocs, params, "templateId");
        List<RatedRequest> ratedRequests = Arrays.asList(request);

        expectThrows(IllegalStateException.class,
                () -> new RankEvalSpec(ratedRequests, new Precision()));
    }
}
