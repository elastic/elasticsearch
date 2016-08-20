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

    private static Injector injector;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    protected static AggregatorParsers aggParsers;
    protected static IndicesQueriesRegistry queriesRegistry;
    protected static ParseFieldMatcher parseFieldMatcher;

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        // we have to prefer CURRENT since with the range of versions we support
        // it's rather unlikely to get the current actually.
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        Settings settings = Settings.builder().put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false).build();

        index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        final ThreadPool threadPool = new ThreadPool(settings);
        final ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, new ClusterState.Builder(clusterService.state()).metaData(new MetaData.Builder()
                .put(new IndexMetaData.Builder(index.getName()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0))));
        ScriptModule scriptModule = newTestScriptModule();
        List<Setting<?>> scriptSettings = scriptModule.getSettings();
        scriptSettings.add(InternalSettingsPlugin.VERSION_CREATED);
        SettingsModule settingsModule = new SettingsModule(settings, scriptSettings, Collections.emptyList());

        IndicesModule indicesModule = new IndicesModule(Collections.emptyList()) {
            @Override
            protected void configure() {
                bindMapperExtension();
            }
        };
        SearchModule searchModule = new SearchModule(settings, false, emptyList()) {
            @Override
            protected void configureSearch() {
                // Skip me
            }
        };
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        injector = new ModulesBuilder().add(
            (b) -> {
                b.bind(Environment.class).toInstance(new Environment(settings));
                b.bind(ThreadPool.class).toInstance(threadPool);
                b.bind(ScriptService.class).toInstance(scriptModule.getScriptService());
            },
            settingsModule, indicesModule, searchModule,
            new IndexSettingsModule(index, settings),
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(ClusterService.class).toInstance(clusterService);
                    bind(CircuitBreakerService.class).toInstance(new NoneCircuitBreakerService());
                    bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                }
            }).createInjector();
        aggParsers = injector.getInstance(SearchRequestParsers.class).aggParsers;
        // create some random type with some default field, those types will
        // stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
        queriesRegistry = injector.getInstance(IndicesQueriesRegistry.class);
        parseFieldMatcher = ParseFieldMatcher.STRICT;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        injector.getInstance(ClusterService.class).close();
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        aggParsers = null;
        currentTypes = null;
        namedWriteableRegistry = null;
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
