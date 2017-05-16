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
package org.elasticsearch.legacy.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.legacy.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.common.inject.AbstractModule;
import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.common.inject.ModulesBuilder;
import org.elasticsearch.legacy.common.inject.util.Providers;
import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsModule;
import org.elasticsearch.legacy.common.xcontent.XContentFactory;
import org.elasticsearch.legacy.common.xcontent.XContentParser;
import org.elasticsearch.legacy.env.Environment;
import org.elasticsearch.legacy.env.EnvironmentModule;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.IndexNameModule;
import org.elasticsearch.legacy.index.analysis.AnalysisModule;
import org.elasticsearch.legacy.index.cache.IndexCacheModule;
import org.elasticsearch.legacy.index.codec.CodecModule;
import org.elasticsearch.legacy.index.engine.IndexEngineModule;
import org.elasticsearch.legacy.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.legacy.index.settings.IndexSettingsModule;
import org.elasticsearch.legacy.index.similarity.SimilarityModule;
import org.elasticsearch.legacy.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.legacy.indices.fielddata.breaker.NoneCircuitBreakerService;
import org.elasticsearch.legacy.indices.query.IndicesQueriesModule;
import org.elasticsearch.legacy.script.ScriptModule;
import org.elasticsearch.legacy.test.ElasticsearchTestCase;
import org.elasticsearch.legacy.threadpool.ThreadPoolModule;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Test parsing and executing a template request.
 */
public class TemplateQueryParserTest extends ElasticsearchTestCase {

    private Injector injector;
    private QueryParseContext context;

    @Before
    public void setup() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.conf", this.getResource("config").getPath())
                .put("name", getClass().getName())
                .build();

        Index index = new Index("test");
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
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

        IndexQueryParserService queryParserService = injector.getInstance(IndexQueryParserService.class);
        context = new QueryParseContext(index, queryParserService);
    }

    @Test
    public void testParser() throws IOException {
        String templateString = "{\"template\": {"
                + "\"query\":{\"match_{{template}}\": {}},"
                + "\"params\":{\"template\":\"all\"}}" + "}";

        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        Query query = parser.parse(context);
        assertTrue("Parsing template query failed.", query instanceof ConstantScoreQuery);
    }

    @Test
    public void testParserCanExtractTemplateNames() throws Exception {
        String templateString = "{ \"template\": { \"file\": \"storedTemplate\" ,\"params\":{\"template\":\"all\" } } } ";

        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        Query query = parser.parse(context);
        assertTrue("Parsing template query failed.", query instanceof ConstantScoreQuery);
    }
}
