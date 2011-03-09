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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleIcuAnalysisTests {

    @Test public void testDefaultsIcuAnalysis() {
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, EMPTY_SETTINGS),
                new IndexNameModule(index),
                new AnalysisModule(EMPTY_SETTINGS).addProcessor(new IcuAnalysisBinderProcessor())).createInjector();

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("icu_normalizer");
        MatcherAssert.assertThat(filterFactory, instanceOf(IcuNormalizerTokenFilterFactory.class));
    }
}
