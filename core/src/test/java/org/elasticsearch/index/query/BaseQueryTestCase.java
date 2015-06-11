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

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@Ignore
public abstract class BaseQueryTestCase<QB extends QueryBuilder<QB>> extends ElasticsearchTestCase {

    protected static final String OBJECT_FIELD_NAME = "object";
    protected static final String DATE_FIELD_NAME = "age";
    protected static final String INT_FIELD_NAME = "price";
    protected static final String STRING_FIELD_NAME = "text";
    protected static final String DOUBLE_FIELD_NAME = "double";
    protected static final String BOOLEAN_FIELD_NAME = "boolean";
    protected static final String[] mappedFieldNames = new String[] { DATE_FIELD_NAME, INT_FIELD_NAME, STRING_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, OBJECT_FIELD_NAME };

    private static Injector injector;
    private static IndexQueryParserService queryParserService;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * Setup for the whole base test class.
     * @throws IOException
     */
    @BeforeClass
    public static void init() throws IOException {
        Settings settings = Settings.settingsBuilder()
                .put("name", BaseQueryTestCase.class.toString())
                .put("path.home", createTempDir())
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(),
                        Version.V_1_0_0, Version.CURRENT))
                .build();

        index = new Index("test");
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(new ThreadPool(settings)),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new SimilarityModule(settings),
                new IndexNameModule(index),
                new FunctionScoreModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        bind(NamedWriteableRegistry.class).asEagerSingleton();
                    }
                }
        ).createInjector();
        queryParserService = injector.getInstance(IndexQueryParserService.class);
        MapperService mapperService = queryParserService.mapperService;
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                    DATE_FIELD_NAME, "type=date",
                    INT_FIELD_NAME, "type=integer",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    STRING_FIELD_NAME, "type=string",
                    OBJECT_FIELD_NAME, "type=object",
                    OBJECT_FIELD_NAME+"."+DATE_FIELD_NAME, "type=date",
                    OBJECT_FIELD_NAME+"."+INT_FIELD_NAME, "type=integer").string()), false);
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
    }

    @Before
    public void beforeTest() {
        //set some random types to be queried as part the search request, before each test
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
        //some query (e.g. range query) have a different behaviour depending on whether the current search context is set or not
        //which is why we randomly set the search context, which will internally also do QueryParseContext.setTypes(types)
        if (randomBoolean()) {
            QueryParseContext.setTypes(types);
        } else {
            TestSearchContext testSearchContext = new TestSearchContext();
            testSearchContext.setTypes(types);
            SearchContext.setCurrent(testSearchContext);
        }
    }

    @After
    public void afterTest() {
        QueryParseContext.removeTypes();
        SearchContext.removeCurrent();
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB createTestQueryBuilder();

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    @Test
    @Repeat(iterations = 20)
    public void testFromXContent() throws IOException {
        QB testQuery = createTestQueryBuilder();
        QueryParseContext context = createContext();
        String contentString = testQuery.toString();
        XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
        context.reset(parser);
        assertQueryHeader(parser, testQuery.queryId());

        QueryBuilder newQuery = queryParserService.queryParser(testQuery.queryId()).fromXContent(context);
        assertNotSame(newQuery, testQuery);
        assertEquals(newQuery, testQuery);
        assertEquals(newQuery.hashCode(), testQuery.hashCode());
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    @Test
    @Repeat(iterations = 20)
    public void testToQuery() throws IOException {
        QB testQuery = createTestQueryBuilder();
        QueryParseContext context = createContext();
        context.setAllowUnmappedFields(true);

        Query expectedQuery = createExpectedQuery(testQuery, context);
        Query actualQuery = testQuery.toQuery(context);
        assertThat(actualQuery, instanceOf(expectedQuery.getClass()));
        assertThat(actualQuery, equalTo(expectedQuery));
        assertLuceneQuery(testQuery, actualQuery, context);
    }

    /**
     * Creates the expected lucene query given the current {@link QueryBuilder} and {@link QueryParseContext}.
     * The returned query will be compared with the result of {@link QueryBuilder#toQuery(QueryParseContext)} to test its behaviour.
     */
    protected abstract Query createExpectedQuery(QB queryBuilder, QueryParseContext context) throws IOException;

    /**
     * Run after default equality comparison between lucene expected query and result of {@link QueryBuilder#toQuery(QueryParseContext)}.
     * Can contain additional assertions that are query specific. Empty default implementation.
     */
    protected void assertLuceneQuery(QB queryBuilder, Query query, QueryParseContext context) {

    }

    /**
     * Test serialization and deserialization of the test query.
     */
    @Test
    @Repeat(iterations = 20)
    public void testSerialization() throws IOException {
        QB testQuery = createTestQueryBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testQuery.writeTo(output);
            try (StreamInput in = new FilterStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                QueryBuilder prototype = queryParserService.queryParser(testQuery.queryId()).getBuilderPrototype();
                QueryBuilder deserializedQuery = prototype.readFrom(in);
                assertEquals(deserializedQuery, testQuery);
                assertEquals(deserializedQuery.hashCode(), testQuery.hashCode());
                assertNotSame(deserializedQuery, testQuery);
            }
        }
    }

    /**
     * @return a new {@link QueryParseContext} based on the base test index and queryParserService
     */
    protected static QueryParseContext createContext() {
        return new QueryParseContext(index, queryParserService);
    }

    protected static void assertQueryHeader(XContentParser parser, String expectedParserName) throws IOException {
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), is(expectedParserName));
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
    }
}
