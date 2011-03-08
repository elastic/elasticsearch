/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent.plugin;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.xcontent.XContentIndexQueryParser;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexQueryParserPluginTests {

    @Test public void testCustomInjection() {
        Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        IndexQueryParserModule queryParserModule = new IndexQueryParserModule(settings);
        queryParserModule.addProcessor(new IndexQueryParserModule.QueryParsersProcessor() {
            @Override public void processXContentQueryParsers(XContentQueryParsersBindings bindings) {
                bindings.processXContentQueryParser("my", PluginJsonQueryParser.class);
            }

            @Override public void processXContentFilterParsers(XContentFilterParsersBindings bindings) {
                bindings.processXContentQueryFilter("my", PluginJsonFilterParser.class);
            }
        });

        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new ThreadPoolModule(settings),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
                new SimilarityModule(settings),
                queryParserModule,
                new IndexNameModule(index)
        ).createInjector();
        IndexQueryParserService indexQueryParserService = injector.getInstance(IndexQueryParserService.class);


        XContentIndexQueryParser indexQueryParser = ((XContentIndexQueryParser) indexQueryParserService.defaultIndexQueryParser());

        PluginJsonQueryParser myJsonQueryParser = (PluginJsonQueryParser) indexQueryParser.queryParser("my");

        assertThat(myJsonQueryParser.names()[0], equalTo("my"));

        PluginJsonFilterParser myJsonFilterParser = (PluginJsonFilterParser) indexQueryParser.filterParser("my");
        assertThat(myJsonFilterParser.names()[0], equalTo("my"));
    }
}