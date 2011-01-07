/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.percolator;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.script.ScriptModule;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimplePercolatorTests {

    private PercolatorService percolatorService;

    @BeforeTest public void buildPercolatorService() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "none")
                .build();
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new ScriptModule(),
                new MapperServiceModule(),
                new IndexSettingsModule(settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
                new SimilarityModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index),
                new PercolatorModule()
        ).createInjector();

        percolatorService = injector.getInstance(PercolatorService.class);
    }

    @Test public void testSimplePercolator() throws Exception {
        // introduce the doc
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("field1", 1)
                .field("field2", "value")
                .endObject();
        byte[] source = doc.copiedBytes();

        PercolatorService.Response percolate = percolatorService.percolate(new PercolatorService.Request("type1", source));
        assertThat(percolate.matches(), hasSize(0));

        // add a query
        percolatorService.addQuery("test1", termQuery("field2", "value"));

        percolate = percolatorService.percolate(new PercolatorService.Request("type1", source));
        assertThat(percolate.matches(), hasSize(1));
        assertThat(percolate.matches(), hasItem("test1"));

        percolatorService.addQuery("test2", termQuery("field1", 1));

        percolate = percolatorService.percolate(new PercolatorService.Request("type1", source));
        assertThat(percolate.matches(), hasSize(2));
        assertThat(percolate.matches(), hasItems("test1", "test2"));

        percolate = percolatorService.percolate(new PercolatorService.Request("type1", source).match("*2"));
        assertThat(percolate.matches(), hasSize(1));
        assertThat(percolate.matches(), hasItems("test2"));

        percolate = percolatorService.percolate(new PercolatorService.Request("type1", source).match("*").unmatch("*1"));
        assertThat(percolate.matches(), hasSize(1));
        assertThat(percolate.matches(), hasItems("test2"));

        percolatorService.removeQuery("test2");
        percolate = percolatorService.percolate(new PercolatorService.Request("type1", source));
        assertThat(percolate.matches(), hasSize(1));
        assertThat(percolate.matches(), hasItems("test1"));
    }
}
