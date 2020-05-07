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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public abstract class AbstractBuilderTestCase extends ESTestCase {

    public static final String TEXT_FIELD_NAME = "mapped_string";
    public static final String TEXT_ALIAS_FIELD_NAME = "mapped_string_alias";
    protected static final String KEYWORD_FIELD_NAME = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String INT_ALIAS_FIELD_NAME = "mapped_int_field_alias";
    protected static final String INT_RANGE_FIELD_NAME = "mapped_int_range";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_NANOS_FIELD_NAME = "mapped_date_nanos";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String DATE_ALIAS_FIELD_NAME = "mapped_date_alias";
    protected static final String DATE_RANGE_FIELD_NAME = "mapped_date_range";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_POINT_ALIAS_FIELD_NAME = "mapped_geo_point_alias";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[]{TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME,
        INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_NANOS_FIELD_NAME, DATE_FIELD_NAME,
        DATE_RANGE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME,
        GEO_SHAPE_FIELD_NAME};
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[]{TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME,
        INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_NANOS_FIELD_NAME,
        DATE_FIELD_NAME, DATE_RANGE_FIELD_NAME,  GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME};

    private static final Map<String, String> ALIAS_TO_CONCRETE_FIELD_NAME = new HashMap<>();
    static {
        ALIAS_TO_CONCRETE_FIELD_NAME.put(TEXT_ALIAS_FIELD_NAME, TEXT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(INT_ALIAS_FIELD_NAME, INT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(DATE_ALIAS_FIELD_NAME, DATE_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(GEO_POINT_ALIAS_FIELD_NAME, GEO_POINT_FIELD_NAME);
    }

    private static ServiceHolder serviceHolder;
    private static ServiceHolder serviceHolderWithNoType;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Index index;
    private static long nowInMillis;

    protected static Index getIndex() {
        return index;
    }

    @SuppressWarnings("deprecation") // dependencies in server for geo_shape field should be decoupled
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(TestGeoShapeFieldMapperPlugin.class);
    }

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
    }

    @BeforeClass
    public static void beforeClass() {
        nodeSettings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        index = new Index(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLength(10));
        nowInMillis = randomNonNegativeLong();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return serviceHolder.xContentRegistry;
    }

    protected NamedWriteableRegistry namedWriteableRegistry() {
        return serviceHolder.namedWriteableRegistry;
    }

    /**
     * make sure query names are unique by suffixing them with increasing counter
     */
    protected static String createUniqueRandomName() {
        String queryName = randomAlphaOfLengthBetween(1, 10) + queryNameId;
        queryNameId++;
        return queryName;
    }

    protected Settings createTestIndexSettings() {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version indexVersionCreated = randomBoolean() ? Version.CURRENT : VersionUtils.randomIndexCompatibleVersion(random());
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionCreated)
            .build();
    }

    protected static IndexSettings indexSettings() {
        return serviceHolder.idxSettings;
    }

    protected static String expectedFieldName(String builderFieldName) {
        return ALIAS_TO_CONCRETE_FIELD_NAME.getOrDefault(builderFieldName, builderFieldName);
    }

    protected Iterable<MappedFieldType> getMapping() {
        return serviceHolder.mapperService.fieldTypes();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(serviceHolder);
        IOUtils.close(serviceHolderWithNoType);
        serviceHolder = null;
        serviceHolderWithNoType = null;
    }

    @Before
    public void beforeTest() throws Exception {
        if (serviceHolder == null) {
            assert serviceHolderWithNoType == null;
            // we initialize the serviceHolder and serviceHolderWithNoType just once, but need some
            // calls to the randomness source during its setup. In order to not mix these calls with
            // the randomness source that is later used in the test method, we use the master seed during
            // this setup
            long masterSeed = SeedUtils.parseSeed(RandomizedTest.getContext().getRunnerSeedAsString());
            RandomizedTest.getContext().runWithPrivateRandomness(masterSeed, (Callable<Void>) () -> {
                serviceHolder = new ServiceHolder(nodeSettings, createTestIndexSettings(), getPlugins(), nowInMillis,
                        AbstractBuilderTestCase.this, true);
                serviceHolderWithNoType = new ServiceHolder(nodeSettings, createTestIndexSettings(), getPlugins(), nowInMillis,
                        AbstractBuilderTestCase.this, false);
                return null;
            });
        }

        serviceHolder.clientInvocationHandler.delegate = this;
        serviceHolderWithNoType.clientInvocationHandler.delegate = this;
    }

    @After
    public void afterTest() {
        serviceHolder.clientInvocationHandler.delegate = null;
        serviceHolderWithNoType.clientInvocationHandler.delegate = null;
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

    /**
     * @return a new {@link QueryShardContext} with the provided searcher
     */
    protected static QueryShardContext createShardContext(IndexSearcher searcher) {
        return serviceHolder.createShardContext(searcher);
    }

    /**
     * @return a new {@link QueryShardContext} based on an index with no type registered
     */
    protected static QueryShardContext createShardContextWithNoType() {
        return serviceHolderWithNoType.createShardContext(null);
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        return createShardContext(null);
    }

    private static class ClientInvocationHandler implements InvocationHandler {
        AbstractBuilderTestCase delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getMethod("get", GetRequest.class, ActionListener.class))){
                GetResponse getResponse = delegate.executeGet((GetRequest) args[0]);
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
                if (randomBoolean()) {
                    listener.onResponse(getResponse);
                } else {
                    new Thread(() -> listener.onResponse(getResponse)).start();
                }
                return null;
            } else if (method.equals(Client.class.getMethod
                ("multiTermVectors", MultiTermVectorsRequest.class))) {
                return new PlainActionFuture<MultiTermVectorsResponse>() {
                    @Override
                    public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                    }
                };
            } else if (method.equals(Object.class.getMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    private static class ServiceHolder implements Closeable {
        private final IndexFieldDataService indexFieldDataService;
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final NamedXContentRegistry xContentRegistry;
        private final ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final BitsetFilterCache bitsetFilterCache;
        private final ScriptService scriptService;
        private final Client client;
        private final long nowInMillis;

        ServiceHolder(Settings nodeSettings,
                        Settings indexSettings,
                        Collection<Class<? extends Plugin>> plugins,
                        long nowInMillis,
                        AbstractBuilderTestCase testCase,
                        boolean registerType) throws IOException {
            this.nowInMillis = nowInMillis;
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, emptyMap(),
                    null, () -> {
                        throw new AssertionError("node.name must be set");
                    });
            PluginsService pluginsService;
            pluginsService = new PluginsService(nodeSettings, null, env.modulesFile(), env.pluginsFile(), plugins);

            client = (Client) Proxy.newProxyInstance(
                    Client.class.getClassLoader(),
                    new Class[]{Client.class},
                    clientInvocationHandler);
            ScriptModule scriptModule = createScriptModule(pluginsService.filterPlugins(ScriptPlugin.class));
            List<Setting<?>> additionalSettings = pluginsService.getPluginSettings();
            SettingsModule settingsModule =
                    new SettingsModule(nodeSettings, additionalSettings, pluginsService.getPluginSettingsFilter(), Collections.emptySet());
            searchModule = new SearchModule(nodeSettings, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(IndicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            namedWriteableRegistry = new NamedWriteableRegistry(entries);
            xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream()
                    ).flatMap(Function.identity()).collect(toList()));
            IndexScopedSettings indexScopedSettings = settingsModule.getIndexScopedSettings();
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), emptyList());
            IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
            scriptService = new MockScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts);
            similarityService = new SimilarityService(idxSettings, null, Collections.emptyMap());
            MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
            mapperService = new MapperService(idxSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                    () -> createShardContext(null), () -> false);
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
            indexFieldDataService = new IndexFieldDataService(idxSettings, indicesFieldDataCache,
                    new NoneCircuitBreakerService(), mapperService);
            bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });

            if (registerType) {
                mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
                    TEXT_FIELD_NAME, "type=text",
                    KEYWORD_FIELD_NAME, "type=keyword",
                    TEXT_ALIAS_FIELD_NAME, "type=alias,path=" + TEXT_FIELD_NAME,
                    INT_FIELD_NAME, "type=integer",
                    INT_ALIAS_FIELD_NAME, "type=alias,path=" + INT_FIELD_NAME,
                    INT_RANGE_FIELD_NAME, "type=integer_range",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    DATE_NANOS_FIELD_NAME, "type=date_nanos",
                    DATE_FIELD_NAME, "type=date",
                    DATE_ALIAS_FIELD_NAME, "type=alias,path=" + DATE_FIELD_NAME,
                    DATE_RANGE_FIELD_NAME, "type=date_range",
                    OBJECT_FIELD_NAME, "type=object",
                    GEO_POINT_FIELD_NAME, "type=geo_point",
                    GEO_POINT_ALIAS_FIELD_NAME, "type=alias,path=" + GEO_POINT_FIELD_NAME,
                    GEO_SHAPE_FIELD_NAME, "type=geo_shape"
                ))), MapperService.MergeReason.MAPPING_UPDATE);
                // also add mappings for two inner field in the object field
                mapperService.merge("_doc", new CompressedXContent("{\"properties\":{\"" + OBJECT_FIELD_NAME + "\":{\"type\":\"object\","
                        + "\"properties\":{\"" + DATE_FIELD_NAME + "\":{\"type\":\"date\"},\"" +
                        INT_FIELD_NAME + "\":{\"type\":\"integer\"}}}}}"),
                    MapperService.MergeReason.MAPPING_UPDATE);
                testCase.initializeAdditionalMappings(mapperService);
            }
        }

        public static Predicate<String> indexNameMatcher() {
            // Simplistic index name matcher used for testing
            return pattern -> Regex.simpleMatch(pattern, index.getName());
        }

        @Override
        public void close() throws IOException {
        }

        QueryShardContext createShardContext(IndexSearcher searcher) {
            return new QueryShardContext(0, idxSettings, BigArrays.NON_RECYCLING_INSTANCE, bitsetFilterCache,
                indexFieldDataService::getForField, mapperService, similarityService, scriptService, xContentRegistry,
                namedWriteableRegistry, this.client, searcher, () -> nowInMillis, null, indexNameMatcher(), () -> true, null);
        }

        ScriptModule createScriptModule(List<ScriptPlugin> scriptPlugins) {
            if (scriptPlugins == null || scriptPlugins.isEmpty()) {
                return new ScriptModule(Settings.EMPTY, singletonList(new ScriptPlugin() {
                    @Override
                    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                        return new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"), emptyMap());
                    }
                }));
            }
            return new ScriptModule(Settings.EMPTY, scriptPlugins);
        }
    }
}
