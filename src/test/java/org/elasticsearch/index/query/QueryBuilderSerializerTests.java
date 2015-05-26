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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
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
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;

public class QueryBuilderSerializerTests extends ElasticsearchTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
                .put("name", "testQueryBuilderSerializerStaticInjection")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", createTempDir()).build();
        Index index = new Index("test");
        new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(threadPool = new ThreadPool(getClass().getName())),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new SimilarityModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index),
                new FunctionScoreModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                    }
                }
        ).createInjector();
    }

    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
    }

    @Test
    public void testQueryBuilderSerializerStaticInjection() throws Exception {
        assertThat(QueryBuilderSerializer.indicesQueriesRegistry, notNullValue());
        assertThat(QueryBuilderSerializer.indicesQueriesRegistry.queryParsers().size(), greaterThan(0));
    }

    @Test
    public void testQueryBuilderSerialization() throws IOException {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        if (randomBoolean()) {
            termQueryBuilder.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            termQueryBuilder.queryName(randomAsciiOfLengthBetween(1, 10));
        }

        Version version = VersionUtils.randomVersion(random());
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        QueryBuilderSerializer.write(termQueryBuilder, out);
        StreamInput in = StreamInput.wrap(out.bytes());
        in.setVersion(version);
        QueryBuilder queryBuilder = QueryBuilderSerializer.read(in);
        assertThat(queryBuilder, equalTo((QueryBuilder)termQueryBuilder));
    }
}
