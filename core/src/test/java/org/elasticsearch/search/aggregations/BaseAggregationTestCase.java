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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.AbstractQueryTestCase;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public abstract class BaseAggregationTestCase<AB extends AggregatorBuilder<AB>> extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String[] mappedFieldNames = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME };

    private static Injector injector;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    private static AggregatorParsers aggParsers;
    private static IndicesQueriesRegistry queriesRegistry;
    private static ParseFieldMatcher parseFieldMatcher;

    protected abstract AB createTestAggregatorBuilder();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        Settings settings = Settings.settingsBuilder()
                .put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .build();

        namedWriteableRegistry =  new NamedWriteableRegistry();
        index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        Settings indexSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        final TestClusterService clusterService = new TestClusterService();
        clusterService.setState(new ClusterState.Builder(clusterService.state()).metaData(new MetaData.Builder()
                .put(new IndexMetaData.Builder(index.getName()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0))));
        SettingsModule settingsModule = new SettingsModule(settings);
        settingsModule.registerSetting(InternalSettingsPlugin.VERSION_CREATED);
        ScriptModule scriptModule = new ScriptModule() {
            @Override
            protected void configure() {
                Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                        // no file watching, so we don't need a
                        // ResourceWatcherService
                        .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false).build();
                MockScriptEngine mockScriptEngine = new MockScriptEngine();
                Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
                multibinder.addBinding().toInstance(mockScriptEngine);
                Set<ScriptEngineService> engines = new HashSet<>();
                engines.add(mockScriptEngine);
                List<ScriptContext.Plugin> customContexts = new ArrayList<>();
                ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections
                        .singletonList(new ScriptEngineRegistry.ScriptEngineRegistration(MockScriptEngine.class, MockScriptEngine.TYPES)));
                bind(ScriptEngineRegistry.class).toInstance(scriptEngineRegistry);
                ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customContexts);
                bind(ScriptContextRegistry.class).toInstance(scriptContextRegistry);
                ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
                bind(ScriptSettings.class).toInstance(scriptSettings);
                try {
                    ScriptService scriptService = new ScriptService(settings, new Environment(settings), engines, null,
                            scriptEngineRegistry, scriptContextRegistry, scriptSettings);
                    bind(ScriptService.class).toInstance(scriptService);
                } catch (IOException e) {
                    throw new IllegalStateException("error while binding ScriptService", e);
                }
            }
        };
        scriptModule.prepareSettings(settingsModule);
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                settingsModule,
                new ThreadPoolModule(new ThreadPool(settings)),
                scriptModule,
                new IndicesModule() {

                    @Override
                    protected void configure() {
                        bindMapperExtension();
                    }
                }, new SearchModule(settings, namedWriteableRegistry) {
                    @Override
                    protected void configureSearch() {
                        // Skip me
                    }
                    @Override
                    protected void configureSuggesters() {
                        // Skip me
                    }
                },
                new IndexSettingsModule(index, settings),

                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of(clusterService));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    }
                }
        ).createInjector();
        aggParsers = injector.getInstance(AggregatorParsers.class);
        //create some random type with some default field, those types will stick around for all of the subclasses
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
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        aggParsers = null;
        currentTypes = null;
        namedWriteableRegistry = null;
    }

    /**
     * Generic test that creates new AggregatorFactory from the test
     * AggregatorFactory and checks both for equality and asserts equality on
     * the two queries.
     */
    public void testFromXContent() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().addAggregator(testAgg);
        String contentString = factoriesBuilder.toString();
        XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry);
        parseContext.reset(parser);
        parseContext.parseFieldMatcher(parseFieldMatcher);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.name, parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.type.name(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorBuilder newAgg = aggParsers.parser(testAgg.getType()).parse(testAgg.name, parser, parseContext);
        assertSame(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    /**
     * Test serialization and deserialization of the test AggregatorFactory.
     */

    public void testSerialization() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testAgg.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                AggregatorBuilder prototype = (AggregatorBuilder) namedWriteableRegistry.getPrototype(AggregatorBuilder.class,
                    testAgg.getWriteableName());
                AggregatorBuilder deserializedQuery = prototype.readFrom(in);
                assertEquals(deserializedQuery, testAgg);
                assertEquals(deserializedQuery.hashCode(), testAgg.hashCode());
                assertNotSame(deserializedQuery, testAgg);
            }
        }
    }


    public void testEqualsAndHashcode() throws IOException {
        AB firstAgg = createTestAggregatorBuilder();
        assertFalse("aggregation is equal to null", firstAgg.equals(null));
        assertFalse("aggregation is equal to incompatible type", firstAgg.equals(""));
        assertTrue("aggregation is not equal to self", firstAgg.equals(firstAgg));
        assertThat("same aggregation's hashcode returns different values if called multiple times", firstAgg.hashCode(),
                equalTo(firstAgg.hashCode()));

        AB secondQuery = copyAggregation(firstAgg);
        assertTrue("aggregation is not equal to self", secondQuery.equals(secondQuery));
        assertTrue("aggregation is not equal to its copy", firstAgg.equals(secondQuery));
        assertTrue("equals is not symmetric", secondQuery.equals(firstAgg));
        assertThat("aggregation copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(firstAgg.hashCode()));

        AB thirdQuery = copyAggregation(secondQuery);
        assertTrue("aggregation is not equal to self", thirdQuery.equals(thirdQuery));
        assertTrue("aggregation is not equal to its copy", secondQuery.equals(thirdQuery));
        assertThat("aggregation copy's hashcode is different from original hashcode", secondQuery.hashCode(),
                equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not transitive", firstAgg.equals(thirdQuery));
        assertThat("aggregation copy's hashcode is different from original hashcode", firstAgg.hashCode(), equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not symmetric", thirdQuery.equals(secondQuery));
        assertTrue("equals is not symmetric", thirdQuery.equals(firstAgg));
    }

    // we use the streaming infra to create a copy of the query provided as
    // argument
    private AB copyAggregation(AB agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            agg.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                AggregatorBuilder prototype = (AggregatorBuilder) namedWriteableRegistry.getPrototype(AggregatorBuilder.class,
                    agg.getWriteableName());
                @SuppressWarnings("unchecked")
                AB secondAgg = (AB) prototype.readFrom(in);
                return secondAgg;
            }
        }
    }

    protected String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[] { MetaData.ALL };
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    public String randomNumericField() {
        int randomInt = randomInt(3);
        switch (randomInt) {
        case 0:
            return DATE_FIELD_NAME;
        case 1:
            return DOUBLE_FIELD_NAME;
        case 2:
        default:
            return INT_FIELD_NAME;
        }
    }
}
