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

package org.elasticsearch.index.query.guice;

import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.fielddata.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class IndexQueryParserModuleTests extends ElasticsearchTestCase {

    @Test
    public void testCustomInjection() {
        Settings settings = settingsBuilder()
                .put("index.queryparser.query.my.type", MyJsonQueryParser.class)
                .put("index.queryparser.query.my.param1", "value1")
                .put("index.queryparser.filter.my.type", MyJsonFilterParser.class)
                .put("index.queryparser.filter.my.param2", "value2")
                .put("index.cache.filter.type", "none")
                .put("name", "IndexQueryParserModuleTests")
                .build();

        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new CacheRecyclerModule(settings),
                new CodecModule(settings),
                new ThreadPoolModule(settings),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
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

        IndexQueryParserService indexQueryParserService = injector.getInstance(IndexQueryParserService.class);

        MyJsonQueryParser myJsonQueryParser = (MyJsonQueryParser) indexQueryParserService.queryParser("my");

        assertThat(myJsonQueryParser.names()[0], equalTo("my"));
        assertThat(myJsonQueryParser.settings().get("param1"), equalTo("value1"));

        MyJsonFilterParser myJsonFilterParser = (MyJsonFilterParser) indexQueryParserService.filterParser("my");
        assertThat(myJsonFilterParser.names()[0], equalTo("my"));
        assertThat(myJsonFilterParser.settings().get("param2"), equalTo("value2"));

        injector.getInstance(ThreadPool.class).shutdownNow();
    }
}
