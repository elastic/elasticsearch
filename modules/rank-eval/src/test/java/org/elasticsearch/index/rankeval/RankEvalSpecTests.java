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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class RankEvalSpecTests extends ESTestCase {
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

    public void testRoundtripping() throws IOException {
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
        List<RatedRequest> specs = new ArrayList<>();
        size = randomIntBetween(1, 2); // TODO I guess requests with no query spec should be rejected...
        for (int i = 0; i < size; i++) {
            specs.add(RatedRequestsTests.createTestItem(indices, types));
        }

        RankedListQualityMetric metric;
        if (randomBoolean()) {
            metric = PrecisionAtNTests.createTestItem();
        } else {
            metric = DiscountedCumulativeGainAtTests.createTestItem();
        }

        RankEvalSpec testItem = new RankEvalSpec(specs, metric);

        XContentType contentType = ESTestCase.randomFrom(XContentType.values());
        XContent xContent = contentType.xContent();

        if (randomBoolean()) {
            final Map<String, Object> params = randomBoolean() ? null : Collections.singletonMap("key", "value");
            ScriptType scriptType = randomFrom(ScriptType.values());
            String script;
            if (scriptType == ScriptType.INLINE) {
                try (XContentBuilder builder = XContentBuilder.builder(xContent)) {
                    builder.startObject();
                    builder.field("field", randomAsciiOfLengthBetween(1, 5));
                    builder.endObject();
                    script = builder.string();
                }
            } else {
                script = randomAsciiOfLengthBetween(1, 5);
            }

            testItem.setTemplate(new Script(
                        script,
                        scriptType,
                        randomFrom("_lang1", "_lang2", null),
                        params,
                        scriptType == ScriptType.INLINE ? xContent.type() : null));
        }

        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (ESTestCase.randomBoolean()) {
            builder.prettyPrint();
        }
        testItem.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = ESTestCase.shuffleXContent(builder);
        XContentParser itemParser = XContentHelper.createParser(shuffled.bytes());

        QueryParseContext queryContext = new QueryParseContext(searchRequestParsers.queryParsers, itemParser, ParseFieldMatcher.STRICT);
        RankEvalContext rankContext = new RankEvalContext(ParseFieldMatcher.STRICT, queryContext,
                searchRequestParsers, null);

        RankEvalSpec parsedItem = RankEvalSpec.parse(itemParser, rankContext, false);
        // IRL these come from URL parameters - see RestRankEvalAction
        parsedItem.getSpecifications().stream().forEach(e -> {e.setIndices(indices); e.setTypes(types);});
        assertNotSame(testItem, parsedItem);
        assertEquals(testItem, parsedItem);
        assertEquals(testItem.hashCode(), parsedItem.hashCode());
    }

}
