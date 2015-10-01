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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
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
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Test parsing and executing a template request.
 */
// NOTE: this can't be migrated to ESSingleNodeTestCase because of the custom path.conf
public class TemplateQueryParserTests extends ESTestCase {

    private Injector injector;
    private QueryParseContext context;

    @Before
    public void setup() throws IOException {
        Settings settings = Settings.settingsBuilder()
                .put("path.home", createTempDir().toString())
                .put("path.conf", this.getDataPath("config"))
                .put("name", getClass().getName())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();

        Index index = new Index("test");
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(new ThreadPool(settings)),
                new IndicesModule(settings) {
                    @Override
                    public void configure() {
                        // skip services
                        bindQueryParsersExtension();
                    }
                },
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings, new IndicesAnalysisService(settings)),
                new SimilarityModule(settings),
                new IndexNameModule(index),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                    }
                }
        ).createInjector();

        IndexQueryParserService queryParserService = injector.getInstance(IndexQueryParserService.class);
        context = new QueryParseContext(index, queryParserService);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(injector.getInstance(ThreadPool.class));
    }

    @Test
    public void testParser() throws IOException {
        String templateString = "{" + "\"query\":{\"match_{{template}}\": {}}," + "\"params\":{\"template\":\"all\"}" + "}";

        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);
        templateSourceParser.nextToken();

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        Query query = parser.parse(context);
        assertTrue("Parsing template query failed.", query instanceof MatchAllDocsQuery);
    }

    @Test
    public void testParseTemplateAsSingleStringWithConditionalClause() throws IOException {
        String templateString = "{" + "  \"inline\" : \"{ \\\"match_{{#use_it}}{{template}}{{/use_it}}\\\":{} }\"," + "  \"params\":{"
                + "    \"template\":\"all\"," + "    \"use_it\": true" + "  }" + "}";
        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        Query query = parser.parse(context);
        assertTrue("Parsing template query failed.", query instanceof MatchAllDocsQuery);
    }

    /**
     * Test that the template query parser can parse and evaluate template
     * expressed as a single string but still it expects only the query
     * specification (thus this test should fail with specific exception).
     */
    @Test(expected = QueryParsingException.class)
    public void testParseTemplateFailsToParseCompleteQueryAsSingleString() throws IOException {
        String templateString = "{" + "  \"inline\" : \"{ \\\"size\\\": \\\"{{size}}\\\", \\\"query\\\":{\\\"match_all\\\":{}}}\","
                + "  \"params\":{" + "    \"size\":2" + "  }\n" + "}";

        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        parser.parse(context);
    }

    @Test
    public void testParserCanExtractTemplateNames() throws Exception {
        String templateString = "{ \"file\": \"storedTemplate\" ,\"params\":{\"template\":\"all\" } } ";

        XContentParser templateSourceParser = XContentFactory.xContent(templateString).createParser(templateString);
        context.reset(templateSourceParser);
        templateSourceParser.nextToken();

        TemplateQueryParser parser = injector.getInstance(TemplateQueryParser.class);
        Query query = parser.parse(context);
        assertTrue("Parsing template query failed.", query instanceof MatchAllDocsQuery);
    }
}
