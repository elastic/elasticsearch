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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchExtRegistry;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;

public class AggregatorParsingTests extends ESTestCase {

    private String[] currentTypes;

    protected String[] getCurrentTypes() {
        return currentTypes;
    }

    protected AggregatorParsers aggParsers;
    protected IndicesQueriesRegistry queriesRegistry;
    protected ParseFieldMatcher parseFieldMatcher;

    /**
     * Setup for the whole base test class.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // we have to prefer CURRENT since with the range of versions we support
        // it's rather unlikely to get the current actually.
        Settings settings = Settings.builder().put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false).build();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList()) ;
        SearchModule searchModule = new SearchModule(settings, false, emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        aggParsers = searchModule.getSearchRequestParsers().aggParsers;
        // create some random type with some default field, those types will
        // stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
        queriesRegistry = searchModule.getQueryParserRegistry();
        parseFieldMatcher = ParseFieldMatcher.STRICT;
    }

    public void testTwoTypes() throws Exception {
        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (ParsingException e) {
            assertThat(e.toString(), containsString("Found two aggregation type definitions in [in_stock]: [filter] and [terms]"));
        }
    }

    public void testTwoAggs() throws Exception {
        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (ParsingException e) {
            assertThat(e.toString(), containsString("Found two sub aggregation definitions under [by_date]"));
        }
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

        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (ParsingException e) {
            assertThat(e.toString(), containsString("Invalid aggregation name [" + name + "]"));
        }
    }

    public void testSameAggregationName() throws Exception {
        final String name = randomAsciiOfLengthBetween(1, 10);
        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("Two sibling aggregations cannot have the same name: [" + name + "]"));
        }
    }

    public void testMissingName() throws Exception {
        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (ParsingException e) {
            // All Good
        }
    }

    public void testMissingType() throws Exception {
        String source = JsonXContent.contentBuilder()
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
                .endObject().string();
        try {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            aggParsers.parseAggregators(parseContext);
            fail();
        } catch (ParsingException e) {
            // All Good
        }
    }
}
