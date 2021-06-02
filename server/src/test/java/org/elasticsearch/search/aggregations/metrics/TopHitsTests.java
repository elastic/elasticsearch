/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilderTests;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class TopHitsTests extends BaseAggregationTestCase<TopHitsAggregationBuilder> {

    @Override
    protected final TopHitsAggregationBuilder createTestAggregatorBuilder() {
        TopHitsAggregationBuilder factory = new TopHitsAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            factory.from(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            factory.size(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            factory.explain(randomBoolean());
        }
        if (randomBoolean()) {
            factory.version(randomBoolean());
        }
        if (randomBoolean()) {
            factory.seqNoAndPrimaryTerm(randomBoolean());
        }
        if (randomBoolean()) {
            factory.trackScores(randomBoolean());
        }
        switch (randomInt(3)) {
            case 0:
                break;
            case 1:
                factory.storedField("_none_");
                break;
            case 2:
                factory.storedFields(Collections.emptyList());
                break;
            case 3:
                int fieldsSize = randomInt(25);
                List<String> fields = new ArrayList<>(fieldsSize);
                for (int i = 0; i < fieldsSize; i++) {
                    fields.add(randomAlphaOfLengthBetween(5, 50));
                }
                factory.storedFields(fields);
                break;
            default:
                throw new IllegalStateException();
        }
        if (randomBoolean()) {
            int fieldDataFieldsSize = randomInt(25);
            for (int i = 0; i < fieldDataFieldsSize; i++) {
                factory.docValueField(randomAlphaOfLengthBetween(5, 50));
            }
        }
        if (randomBoolean()) {
            int fetchFieldsSize = randomInt(25);
            for (int i = 0; i < fetchFieldsSize; i++) {
                factory.fetchField(randomAlphaOfLengthBetween(5, 50));
            }
        }
        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                if (randomBoolean()) {
                    factory.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"), randomBoolean());
                } else {
                    factory.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"));
                }
            }
        }
        if (randomBoolean()) {
            FetchSourceContext fetchSourceContext;
            int branch = randomInt(5);
            String[] includes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < includes.length; i++) {
                includes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            String[] excludes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < excludes.length; i++) {
                excludes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            switch (branch) {
                case 0:
                    fetchSourceContext = new FetchSourceContext(randomBoolean());
                    break;
                case 1:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 2:
                    fetchSourceContext = new FetchSourceContext(true, new String[]{randomAlphaOfLengthBetween(5, 20)},
                        new String[]{randomAlphaOfLengthBetween(5, 20)});
                    break;
                case 3:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 4:
                    fetchSourceContext = new FetchSourceContext(true, includes, null);
                    break;
                case 5:
                    fetchSourceContext = new FetchSourceContext(true, new String[] {randomAlphaOfLengthBetween(5, 20)}, null);
                    break;
                default:
                    throw new IllegalStateException();
            }
            factory.fetchSource(fetchSourceContext);
        }
        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                case 0:
                    factory.sort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                    break;
                case 1:
                    factory.sort(SortBuilders.geoDistanceSort(randomAlphaOfLengthBetween(5, 20), AbstractQueryTestCase.randomGeohash(1, 12))
                            .order(randomFrom(SortOrder.values())));
                    break;
                case 2:
                    factory.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                    break;
                case 3:
                    factory.sort(SortBuilders.scriptSort(mockScript("foo"), ScriptSortType.NUMBER).order(randomFrom(SortOrder.values())));
                    break;
                case 4:
                    factory.sort(randomAlphaOfLengthBetween(5, 20));
                    break;
                case 5:
                    factory.sort(randomAlphaOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                    break;
                }
            }
        }
        if (randomBoolean()) {
            // parent test shuffles xContent, we need to make sure highlight fields are ordered
            factory.highlighter(
                    HighlightBuilderTests.randomHighlighterBuilder().useExplicitFieldOrder(true));
        }
        return factory;
    }


    public void testFailWithSubAgg() throws Exception {
        String source = "{\n" +
            "    \"top-tags\": {\n" +
            "      \"terms\": {\n" +
            "        \"field\": \"tags\"\n" +
            "      },\n" +
            "      \"aggs\": {\n" +
            "        \"top_tags_hits\": {\n" +
            "          \"top_hits\": {},\n" +
            "          \"aggs\": {\n" +
            "            \"max\": {\n" +
            "              \"max\": {\n" +
            "                \"field\": \"age\"\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(AggregationInitializationException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Aggregator [top_tags_hits] of type [top_hits] cannot accept sub-aggregations"));
    }

    @Override
    protected void assertToXContentAfterSerialization(TopHitsAggregationBuilder original, TopHitsAggregationBuilder deserialized)
        throws IOException {
        ElasticsearchAssertions.assertToXContentEquivalent(
            XContentHelper.toXContent(original, XContentType.JSON, false),
            XContentHelper.toXContent(deserialized, XContentType.JSON, false),
            XContentType.JSON
        );
    }
}
