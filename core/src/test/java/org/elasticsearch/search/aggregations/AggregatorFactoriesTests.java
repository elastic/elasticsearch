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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AggregatorFactoriesTests extends ESTestCase {
    private String[] currentTypes;

    private NamedXContentRegistry xContentRegistry;
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // we have to prefer CURRENT since with the range of versions we support
        // it's rather unlikely to get the current actually.
        Settings settings = Settings.builder().put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false).build();
        // create some random type with some default field, those types will
        // stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
        xContentRegistry = new NamedXContentRegistry(new SearchModule(settings, false, emptyList()).getNamedXContents());
    }

    public void testGetAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo"));
        List<AggregationBuilder> aggregatorFactories = builder.getAggregatorFactories();
        assertThat(aggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class, () -> aggregatorFactories.add(AggregationBuilders.avg("bar")));
    }

    public void testGetPipelineAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addPipelineAggregator(
            PipelineAggregatorBuilders.avgBucket("foo", "path1"));
        List<PipelineAggregationBuilder> pipelineAggregatorFactories = builder.getPipelineAggregatorFactories();
        assertThat(pipelineAggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class,
            () -> pipelineAggregatorFactories.add(PipelineAggregatorBuilders.avgBucket("bar", "path2")));
    }

    public void testTwoTypes() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("in_stock")
                        .startObject("filter")
                            .startObject("range")
                                .startObject("stock")
                                    .field("gt", 0)
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("terms")
                            .field("field", "stock")
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Found two aggregation type definitions in [in_stock]: [filter] and [terms]"));
    }

    public void testTwoAggs() throws Exception {
        assumeFalse("Test only makes sense if XContent parser doesn't have strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());
        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("by_date")
                        .startObject("date_histogram")
                            .field("field", "timestamp")
                            .field("interval", "month")
                        .endObject()
                        .startObject("aggs")
                            .startObject("tag_count")
                                .startObject("cardinality")
                                    .field("field", "tag")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("aggs") // 2nd "aggs": illegal
                            .startObject("tag_count2")
                                .startObject("cardinality")
                                    .field("field", "tag")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Found two sub aggregation definitions under [by_date]"));
    }

    public void testInvalidAggregationName() throws Exception {
        Matcher matcher = Pattern.compile("[^\\[\\]>]+").matcher("");
        String name;
        Random rand = random();
        int len = randomIntBetween(1, 5);
        char[] word = new char[len];
        while (true) {
            for (int i = 0; i < word.length; i++) {
                word[i] = (char) rand.nextInt(127);
            }
            name = String.valueOf(word);
            if (!matcher.reset(name).matches()) {
                break;
            }
        }

        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject(name)
                        .startObject("filter")
                            .startObject("range")
                                .startObject("stock")
                                    .field("gt", 0)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Invalid aggregation name [" + name + "]"));
    }

    public void testSameAggregationName() throws Exception {
        assumeFalse("Test only makes sense if XContent parser doesn't have strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());
        final String name = randomAsciiOfLengthBetween(1, 10);
        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject(name)
                        .startObject("terms")
                            .field("field", "a")
                        .endObject()
                    .endObject()
                    .startObject(name)
                        .startObject("terms")
                            .field("field", "b")
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Two sibling aggregations cannot have the same name: [" + name + "]"));
    }

    public void testMissingName() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("by_date")
                        .startObject("date_histogram")
                            .field("field", "timestamp")
                            .field("interval", "month")
                        .endObject()
                        .startObject("aggs")
                            // the aggregation name is missing
                            //.startObject("tag_count")
                            .startObject("cardinality")
                                .field("field", "tag")
                            .endObject()
                            //.endObject()
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [cardinality]"));
    }

    public void testMissingType() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("by_date")
                        .startObject("date_histogram")
                            .field("field", "timestamp")
                            .field("interval", "month")
                        .endObject()
                        .startObject("aggs")
                            .startObject("tag_count")
                                // the aggregation type is missing
                                //.startObject("cardinality")
                                .field("field", "tag")
                                //.endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        XContentParser parser = createParser(source);
        QueryParseContext parseContext = new QueryParseContext(parser);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parseContext));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [tag_count]"));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
