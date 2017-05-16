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

package org.elasticsearch.legacy.index.analysis;

import org.elasticsearch.legacy.ElasticsearchIllegalArgumentException;
import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.common.inject.ModulesBuilder;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsModule;
import org.elasticsearch.legacy.env.Environment;
import org.elasticsearch.legacy.env.EnvironmentModule;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.IndexNameModule;
import org.elasticsearch.legacy.index.settings.IndexSettingsModule;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.legacy.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import static org.elasticsearch.legacy.common.settings.ImmutableSettings.settingsBuilder;

public class PatternCaptureTokenFilterTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testPatternCaptureTokenFilter() throws Exception {
        Index index = new Index("test");
        Settings settings = settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/pattern_capture.json").build();
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        NamedAnalyzer analyzer1 = analysisService.analyzer("single");

        assertTokenStreamContents(analyzer1.tokenStream("test", "foobarbaz"), new String[]{"foobarbaz","foobar","foo"});

        NamedAnalyzer analyzer2 = analysisService.analyzer("multi");

        assertTokenStreamContents(analyzer2.tokenStream("test", "abc123def"), new String[]{"abc123def","abc","123","def"});

        NamedAnalyzer analyzer3 = analysisService.analyzer("preserve");

        assertTokenStreamContents(analyzer3.tokenStream("test", "foobarbaz"), new String[]{"foobar","foo"});
    }
    
    
    @Test(expected=ElasticsearchIllegalArgumentException.class)
    public void testNoPatterns() {
        new PatternCaptureGroupTokenFilterFactory(new Index("test"), settingsBuilder().build(), "pattern_capture", settingsBuilder().put("pattern", "foobar").build());
    }

}
