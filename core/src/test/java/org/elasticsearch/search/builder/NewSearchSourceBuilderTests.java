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

package org.elasticsearch.search.builder;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.compress.CompressedXContent;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.AbstractQueryTestCase;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder.InnerHit;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class NewSearchSourceBuilderTests extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_SHAPE_FIELD_NAME };
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME };

    private static Injector injector;
    private static IndexQueryParserService queryParserService;

    protected static IndexQueryParserService queryParserService() {
        return queryParserService;
    }

    private static Index index;

    protected static Index getIndex() {
        return index;
    }

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    private static String[] randomTypes;
    private static ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();

    /**
     * Setup for the whole base test class.
     * @throws IOException
     */
    @BeforeClass
    public static void init() throws IOException {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version version = randomBoolean() ? Version.CURRENT : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        Settings settings = Settings.settingsBuilder()
                .put("name", AbstractQueryTestCase.class.toString())
                .put("path.home", createTempDir())
                .build();
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        index = new Index(randomAsciiOfLengthBetween(1, 10));
        final TestClusterService clusterService = new TestClusterService();
        clusterService.setState(new ClusterState.Builder(clusterService.state()).metaData(new MetaData.Builder().put(
                new IndexMetaData.Builder(index.name()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0))));
        final Client proxy = (Client) Proxy.newProxyInstance(Client.class.getClassLoader(), new Class[] { Client.class },
                clientInvocationHandler);
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(new ThreadPool(settings)),
                new MapperServiceModule(),
                new IndicesModule(settings) {
                    @Override
                    public void configure() {
                        // skip services
                        bindQueryParsersExtension();
                    }
                },
                new ScriptModule(settings),
                new IndexSettingsModule(index, indexSettings),
                new IndexCacheModule(indexSettings),
                new AnalysisModule(indexSettings, new IndicesAnalysisService(indexSettings)),
                new SimilarityModule(indexSettings),
                new IndexNameModule(index),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(proxy);
                        Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
                        bind(ClusterService.class).toProvider(Providers.of(clusterService));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        bind(NamedWriteableRegistry.class).asEagerSingleton();
                    }
                }
        ).createInjector();
        queryParserService = injector.getInstance(IndexQueryParserService.class);
        MapperService mapperService = injector.getInstance(MapperService.class);
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                    STRING_FIELD_NAME, "type=string",
                    STRING_FIELD_NAME_2, "type=string",
                    INT_FIELD_NAME, "type=integer",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    DATE_FIELD_NAME, "type=date",
                    OBJECT_FIELD_NAME, "type=object",
                    GEO_POINT_FIELD_NAME, "type=geo_point,lat_lon=true,geohash=true,geohash_prefix=true",
                    GEO_SHAPE_FIELD_NAME, "type=geo_shape"
            ).string()), false, false);
            // also add mappings for two inner field in the object field
            mapperService.merge(type, new CompressedXContent("{\"properties\":{\""+OBJECT_FIELD_NAME+"\":{\"type\":\"object\","
                    + "\"properties\":{\""+DATE_FIELD_NAME+"\":{\"type\":\"date\"},\""+INT_FIELD_NAME+"\":{\"type\":\"integer\"}}}}}"), false, false);
            currentTypes[i] = type;
        }
        namedWriteableRegistry = injector.getInstance(NamedWriteableRegistry.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        queryParserService = null;
        currentTypes = null;
        namedWriteableRegistry = null;
        randomTypes = null;
    }

    @Before
    public void beforeTest() {
        clientInvocationHandler.delegate = this;
        //set some random types to be queried as part the search request, before each test
        randomTypes = getRandomTypes();
    }

    protected void setSearchContext(String[] types) {
        TestSearchContext testSearchContext = new TestSearchContext();
        testSearchContext.setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    @After
    public void afterTest() {
        clientInvocationHandler.delegate = null;
        QueryShardContext.removeTypes();
        SearchContext.removeCurrent();
    }

    protected final SearchSourceBuilder createSearchSourceBuilder() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (randomBoolean()) {
            builder.from(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.size(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.explain(randomBoolean());
        }
        if (randomBoolean()) {
            builder.version(randomBoolean());
        }
        if (randomBoolean()) {
            builder.trackScores(randomBoolean());
        }
        if (randomBoolean()) {
            builder.minScore(randomFloat() * 1000);
        }
        if (randomBoolean()) {
            builder.timeout(new TimeValue(randomIntBetween(1, 100), randomFrom(TimeUnit.values())));
        }
        if (randomBoolean()) {
            builder.terminateAfter(randomIntBetween(1, 100000));
        }
        // if (randomBoolean()) {
        // builder.defaultRescoreWindowSize(randomIntBetween(1, 100));
        // }
        if (randomBoolean()) {
            int fieldsSize = randomInt(25);
            List<String> fields = new ArrayList<>(fieldsSize);
            for (int i = 0; i < fieldsSize; i++) {
                fields.add(randomAsciiOfLengthBetween(5, 50));
            }
            builder.fields(fields);
        }
        if (randomBoolean()) {
            int fieldDataFieldsSize = randomInt(25);
            for (int i = 0; i < fieldDataFieldsSize; i++) {
                builder.fieldDataField(randomAsciiOfLengthBetween(5, 50));
            }
        }
        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                builder.scriptField(randomAsciiOfLengthBetween(5, 50), new Script("foo"));
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
                fetchSourceContext = new FetchSourceContext(randomBoolean(), includes, excludes, randomBoolean());
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
            builder.fetchSource(fetchSourceContext);
        }
        if (randomBoolean()) {
            String[] statsGroups = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < statsGroups.length; i++) {
                statsGroups[i] = randomAsciiOfLengthBetween(5, 20);
            }
            builder.stats(statsGroups);
        }
        if (randomBoolean()) {
            int indexBoostSize = randomIntBetween(1, 10);
            for (int i = 0; i < indexBoostSize; i++) {
                builder.indexBoost(randomAsciiOfLengthBetween(5, 20), randomFloat() * 10);
            }
        }
        if (randomBoolean()) {
            // NORELEASE make RandomQueryBuilder work outside of the
            // AbstractQueryTestCase
            // builder.query(RandomQueryBuilder.createQuery(getRandom()));
            builder.query(QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            // NORELEASE make RandomQueryBuilder work outside of the
            // AbstractQueryTestCase
            // builder.postFilter(RandomQueryBuilder.createQuery(getRandom()));
            builder.postFilter(QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                case 0:
                    builder.sort(SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                    break;
                case 1:
                    builder.sort(SortBuilders.geoDistanceSort(randomAsciiOfLengthBetween(5, 20))
                            .geohashes(AbstractQueryTestCase.randomGeohash(1, 12)).order(randomFrom(SortOrder.values())));
                    break;
                case 2:
                    builder.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                    break;
                case 3:
                    builder.sort(SortBuilders.scriptSort(new Script("foo"), "number").order(randomFrom(SortOrder.values())));
                    break;
                case 4:
                    builder.sort(randomAsciiOfLengthBetween(5, 20));
                    break;
                case 5:
                    builder.sort(randomAsciiOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                    break;
                }
            }
        }
        if (randomBoolean()) {
            // NORELEASE need a random highlight builder method
            builder.highlighter(new HighlightBuilder().field(randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            // NORELEASE need a random suggest builder method
            builder.suggest(new SuggestBuilder().setText(randomAsciiOfLengthBetween(1, 5)).addSuggestion(
                    SuggestBuilders.termSuggestion(randomAsciiOfLengthBetween(1, 5))));
        }
        if (randomBoolean()) {
            // NORELEASE need a random inner hits builder method
            InnerHitsBuilder innerHitsBuilder = new InnerHitsBuilder();
            InnerHit innerHit = new InnerHit();
            innerHit.field(randomAsciiOfLengthBetween(5, 20));
            innerHitsBuilder.addNestedInnerHits(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20), innerHit);
            builder.innerHits(innerHitsBuilder);
        }
        if (randomBoolean()) {
            int numRescores = randomIntBetween(1, 5);
            for (int i = 0; i < numRescores; i++) {
                // NORELEASE need a random rescore builder method
                RescoreBuilder rescoreBuilder = new RescoreBuilder();
                rescoreBuilder.rescorer(RescoreBuilder.queryRescorer(QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20),
                        randomAsciiOfLengthBetween(5, 20))));
                builder.addRescorer(rescoreBuilder);
            }
        }
        if (randomBoolean()) {
            // NORELEASE need a random aggregation builder method
            builder.aggregation(AggregationBuilders.avg(randomAsciiOfLengthBetween(5, 20)));
        }
        return builder;
    }

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    @Test
    public void testFromXContent() throws IOException {
        SearchSourceBuilder testBuilder = createSearchSourceBuilder();
        SearchSourceBuilder newBuilder = parseQuery(testBuilder.toString(), ParseFieldMatcher.STRICT);
        assertNotSame(testBuilder, newBuilder);
        assertEquals(testBuilder, newBuilder);
        assertEquals(testBuilder.hashCode(), newBuilder.hashCode());
    }

    protected SearchSourceBuilder parseQuery(String queryAsString, ParseFieldMatcher matcher) throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsString).createParser(queryAsString);
        QueryParseContext context = createParseContext();
        context.reset(parser);
        context.parseFieldMatcher(matcher);
        System.out.println(queryAsString);
        return SearchSourceBuilder.PROTOTYPE.fromXContent(parser, context);
    }

    /**
     * Test serialization and deserialization of the test query.
     */
    @Test
    public void testSerialization() throws IOException {
        SearchSourceBuilder testBuilder = createSearchSourceBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedBuilder = SearchSourceBuilder.PROTOTYPE.readFrom(in);
                assertEquals(deserializedBuilder, testBuilder);
                assertEquals(deserializedBuilder.hashCode(), testBuilder.hashCode());
                assertNotSame(deserializedBuilder, testBuilder);
            }
        }
    }

    @Test
    public void testEqualsAndHashcode() throws IOException {
        SearchSourceBuilder firstBuilder = createSearchSourceBuilder();
        assertFalse("query is equal to null", firstBuilder.equals(null));
        assertFalse("query is equal to incompatible type", firstBuilder.equals(""));
        assertTrue("query is not equal to self", firstBuilder.equals(firstBuilder));
        assertThat("same query's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                equalTo(firstBuilder.hashCode()));

        SearchSourceBuilder secondBuilder = copyBuilder(firstBuilder);
        assertTrue("query is not equal to self", secondBuilder.equals(secondBuilder));
        assertTrue("query is not equal to its copy", firstBuilder.equals(secondBuilder));
        assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
        assertThat("query copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(firstBuilder.hashCode()));

        SearchSourceBuilder thirdBuilder = copyBuilder(secondBuilder);
        assertTrue("query is not equal to self", thirdBuilder.equals(thirdBuilder));
        assertTrue("query is not equal to its copy", secondBuilder.equals(thirdBuilder));
        assertThat("query copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
        assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
        assertThat("query copy's hashcode is different from original hashcode", firstBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
        assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
        assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
    }

    //we use the streaming infra to create a copy of the query provided as argument
    protected SearchSourceBuilder copyBuilder(SearchSourceBuilder builder) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            builder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                SearchSourceBuilder secondQuery = SearchSourceBuilder.PROTOTYPE.readFrom(in);
                return secondQuery;
            }
        }
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        QueryShardContext queryCreationContext = new QueryShardContext(index, queryParserService);
        queryCreationContext.reset();
        queryCreationContext.parseFieldMatcher(ParseFieldMatcher.EMPTY);
        return queryCreationContext;
    }

    /**
     * @return a new {@link QueryParseContext} based on the base test index and queryParserService
     */
    protected static QueryParseContext createParseContext() {
        return createShardContext().parseContext();
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

    private static class ClientInvocationHandler implements InvocationHandler {
        NewSearchSourceBuilderTests delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getDeclaredMethod("get", GetRequest.class))) {
                return new PlainActionFuture<GetResponse>() {
                    @Override
                    public GetResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeGet((GetRequest) args[0]);
                    }
                };
            } else if (method.equals(Client.class.getDeclaredMethod("multiTermVectors", MultiTermVectorsRequest.class))) {
                return new PlainActionFuture<MultiTermVectorsResponse>() {
                    @Override
                    public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                    }
                };
            } else if (method.equals(Object.class.getDeclaredMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers
     * / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers
     * / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

}
