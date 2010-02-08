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

package org.elasticsearch.index.query.json.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.filter.FilterCacheModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.json.JsonIndexQueryParser;
import org.elasticsearch.index.query.json.JsonQueryParserRegistry;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.util.settings.Settings;
import org.testng.annotations.Test;

import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexQueryParserModuleTests {

    @Test public void testCustomInjection() {
        Settings settings = settingsBuilder()
                .putClass("index.queryparser.json.query.my.type", MyJsonQueryParser.class)
                .put("index.queryparser.json.query.my.param1", "value1")
                .putClass("index.queryparser.json.filter.my.type", MyJsonFilterParser.class)
                .put("index.queryparser.json.filter.my.param2", "value2")
                .put("index.cache.filter.type", "none")
                .build();

        Index index = new Index("test");
        Injector injector = Guice.createInjector(
                new IndexSettingsModule(settings),
                new FilterCacheModule(settings),
                new AnalysisModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index)
        );
        IndexQueryParserService indexQueryParserService = injector.getInstance(IndexQueryParserService.class);


        JsonQueryParserRegistry parserRegistry = ((JsonIndexQueryParser) indexQueryParserService.defaultIndexQueryParser()).queryParserRegistry();

        MyJsonQueryParser myJsonQueryParser = (MyJsonQueryParser) parserRegistry.queryParser("my");

        assertThat(myJsonQueryParser.name(), equalTo("my"));
        assertThat(myJsonQueryParser.settings().get("param1"), equalTo("value1"));

        MyJsonFilterParser myJsonFilterParser = (MyJsonFilterParser) parserRegistry.filterParser("my");
        assertThat(myJsonFilterParser.name(), equalTo("my"));
        assertThat(myJsonFilterParser.settings().get("param2"), equalTo("value2"));
    }
}
