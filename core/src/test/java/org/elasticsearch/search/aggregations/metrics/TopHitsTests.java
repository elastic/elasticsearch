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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilderTests;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class TopHitsTests extends BaseAggregationTestCase<TopHitsAggregationBuilder> {

    @Override
    protected final TopHitsAggregationBuilder createTestAggregatorBuilder() {
        TopHitsAggregationBuilder factory = new TopHitsAggregationBuilder("foo");
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
            factory.trackScores(randomBoolean());
        }
        if (randomBoolean()) {
            int fieldsSize = randomInt(25);
            List<String> fields = new ArrayList<>(fieldsSize);
            for (int i = 0; i < fieldsSize; i++) {
                fields.add(randomAsciiOfLengthBetween(5, 50));
            }
            factory.fields(fields);
        }
        if (randomBoolean()) {
            int fieldDataFieldsSize = randomInt(25);
            for (int i = 0; i < fieldDataFieldsSize; i++) {
                factory.fieldDataField(randomAsciiOfLengthBetween(5, 50));
            }
        }
        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                if (randomBoolean()) {
                    factory.scriptField(randomAsciiOfLengthBetween(5, 50), new Script("foo"), randomBoolean());
                } else {
                    factory.scriptField(randomAsciiOfLengthBetween(5, 50), new Script("foo"));
                }
            }
        }
        if (randomBoolean()) {
            FetchSourceContext fetchSourceContext;
            int branch = randomInt(5);
            String[] includes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < includes.length; i++) {
                includes[i] = randomAsciiOfLengthBetween(5, 20);
            }
            String[] excludes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < excludes.length; i++) {
                excludes[i] = randomAsciiOfLengthBetween(5, 20);
            }
            switch (branch) {
            case 0:
                fetchSourceContext = new FetchSourceContext(randomBoolean());
                break;
            case 1:
                fetchSourceContext = new FetchSourceContext(includes, excludes);
                break;
            case 2:
                fetchSourceContext = new FetchSourceContext(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20));
                break;
            case 3:
                fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                break;
            case 4:
                fetchSourceContext = new FetchSourceContext(includes);
                break;
            case 5:
                fetchSourceContext = new FetchSourceContext(randomAsciiOfLengthBetween(5, 20));
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
                    factory.sort(SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                    break;
                case 1:
                    factory.sort(SortBuilders.geoDistanceSort(randomAsciiOfLengthBetween(5, 20), AbstractQueryTestCase.randomGeohash(1, 12))
                            .order(randomFrom(SortOrder.values())));
                    break;
                case 2:
                    factory.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                    break;
                case 3:
                    factory.sort(SortBuilders.scriptSort(new Script("foo"), ScriptSortType.NUMBER).order(randomFrom(SortOrder.values())));
                    break;
                case 4:
                    factory.sort(randomAsciiOfLengthBetween(5, 20));
                    break;
                case 5:
                    factory.sort(randomAsciiOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                    break;
                }
            }
        }
        if (randomBoolean()) {
            factory.highlighter(HighlightBuilderTests.randomHighlighterBuilder());
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
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (AggregationInitializationException e) {
            assertThat(e.toString(), containsString("Aggregator [top_tags_hits] of type [top_hits] cannot accept sub-aggregations"));
        }
    }

}
