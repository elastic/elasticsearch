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

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.ImmutableSettings;
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
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

@Ignore
public abstract class BaseQueryTestCase<QB extends BaseQueryBuilder & Streamable> extends ElasticsearchTestCase {

    private static int RANDOM_REPS = 20;

    private static Injector injector;
    private static IndexQueryParserService queryParserService;
    private static Index index;

    protected QB testQuery = createTestQueryBuilder();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("name", BaseQueryTestCase.class.toString())
                .put("path.home", createTempDir())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();

        index = new Index("test");
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(settings),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new SimilarityModule(settings),
                new IndexNameModule(index),
                new IndexQueryParserModule(settings),
                new FunctionScoreModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                    }
                }
        ).createInjector();
        queryParserService = injector.getInstance(IndexQueryParserService.class);
    }

    @AfterClass
    public static void after() throws Exception {
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        queryParserService = null;
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB createTestQueryBuilder();

    /**
     * Subclass should handle assertions on the lucene query produced by the query builder under test here
     * @param queryBuilder the original queryBuilder used in this test
     * @param query the lucene query constructed from this
     * @param context the {@link QueryParseContext} that can be used for assertions
     */
    protected abstract void assertLuceneQuery(QB queryBuilder, Query query, QueryParseContext context) throws IOException;

    /**
     * Creates an empty builder of the type of query under test
     */
    protected abstract QB createEmptyQueryBuilder();

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    @Test
    public void testFromXContent() throws IOException {
        for (int i = 0; i < RANDOM_REPS; i++) {
            testQuery = createTestQueryBuilder();
            QueryParseContext context = createContext();
            String contentString = testQuery.toString();
            XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
            context.reset(parser);
            assertQueryHeader(parser, testQuery.parserName());

            QueryBuilder newQuery = queryParserService.queryParser(testQuery.parserName()).fromXContent(context);
            assertNotSame(newQuery, testQuery);
            assertEquals(newQuery, testQuery);
        }
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    @Test
    public void testToQuery() throws IOException {
        for (int i = 0; i < RANDOM_REPS; i++) {
            testQuery = createTestQueryBuilder();
            QueryParseContext context = createContext();
            context.setMapUnmappedFieldAsString(true);
            assertLuceneQuery(testQuery, testQuery.toQuery(context), context);
        }
    }

    /**
     * Test serialization and deserialization of the test query.
     */
    @Test
    public void testSerialization() throws IOException {
        for (int i = 0; i < RANDOM_REPS; i++) {
            testQuery = createTestQueryBuilder();
            BytesStreamOutput output = new BytesStreamOutput();
            testQuery.writeTo(output);

            BytesStreamInput in = new BytesStreamInput(output.bytes());
            QB deserializedQuery = createEmptyQueryBuilder();
            deserializedQuery.readFrom(in);

            assertEquals(deserializedQuery, testQuery);
            assertNotSame(deserializedQuery, testQuery);
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