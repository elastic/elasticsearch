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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;

public abstract class BasePipelineAggregationTestCase<AF extends AbstractPipelineAggregationBuilder<AF>> extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String[] mappedFieldNames = new String[]{STRING_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME};

    private static Injector injector;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    protected static AggregatorParsers aggParsers;
    protected static ParseFieldMatcher parseFieldMatcher;
    protected static IndicesQueriesRegistry queriesRegistry;

    protected abstract AF createTestAggregatorFactory();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        Settings settings = Settings.builder()
                .put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .build();

        namedWriteableRegistry = new NamedWriteableRegistry();
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
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings),threadPool),
                settingsModule,
                scriptModule,
                new IndicesModule(namedWriteableRegistry) {

                    @Override
                    protected void configure() {
                        bindMapperExtension();
                    }
                }, new SearchModule(settings, namedWriteableRegistry) {
                    @Override
                    protected void configureSearch() {
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
        injector.getInstance(ClusterService.class).close();
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
        AF testAgg = createTestAggregatorFactory();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().skipResolveOrder().addPipelineAggregator(testAgg);
        logger.info("Content string: {}", factoriesBuilder);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        XContentParser parser = XContentFactory.xContent(shuffled.bytes()).createParser(shuffled.bytes());
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
        String contentString = factoriesBuilder.toString();
        logger.info("Content string: {}", contentString);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.getName(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.type(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        PipelineAggregationBuilder newAgg = aggParsers.pipelineParser(testAgg.getWriteableName(), ParseFieldMatcher.STRICT)
                .parse(testAgg.getName(), parseContext);
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
        AF testAgg = createTestAggregatorFactory();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                PipelineAggregationBuilder deserializedQuery = in.readNamedWriteable(PipelineAggregationBuilder.class);
                assertEquals(deserializedQuery, testAgg);
                assertEquals(deserializedQuery.hashCode(), testAgg.hashCode());
                assertNotSame(deserializedQuery, testAgg);
            }
        }
    }


    public void testEqualsAndHashcode() throws IOException {
        AF firstAgg = createTestAggregatorFactory();
        assertFalse("aggregation is equal to null", firstAgg.equals(null));
        assertFalse("aggregation is equal to incompatible type", firstAgg.equals(""));
        assertTrue("aggregation is not equal to self", firstAgg.equals(firstAgg));
        assertThat("same aggregation's hashcode returns different values if called multiple times", firstAgg.hashCode(),
                equalTo(firstAgg.hashCode()));

        AF secondQuery = copyAggregation(firstAgg);
        assertTrue("aggregation is not equal to self", secondQuery.equals(secondQuery));
        assertTrue("aggregation is not equal to its copy", firstAgg.equals(secondQuery));
        assertTrue("equals is not symmetric", secondQuery.equals(firstAgg));
        assertThat("aggregation copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(firstAgg.hashCode()));

        AF thirdQuery = copyAggregation(secondQuery);
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
    private AF copyAggregation(AF agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(agg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                @SuppressWarnings("unchecked")
                AF secondAgg = (AF) in.readNamedWriteable(PipelineAggregationBuilder.class);
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
                types = new String[]{MetaData.ALL};
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
