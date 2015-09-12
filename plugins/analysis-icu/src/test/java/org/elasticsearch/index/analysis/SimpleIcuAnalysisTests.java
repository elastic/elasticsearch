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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.analysis.AnalysisTestUtils.createAnalysisService;
import static org.hamcrest.Matchers.instanceOf;
/**
 */
public class SimpleIcuAnalysisTests extends ESTestCase {

    @Test
    public void testDefaultsIcuAnalysis() {
        Settings settings = settingsBuilder()
                .put("path.home", createTempDir()).build();
        AnalysisService analysisService = createAnalysisService(settings);

        TokenizerFactory tokenizerFactory = analysisService.tokenizer("icu_tokenizer");
        assertThat(tokenizerFactory, instanceOf(IcuTokenizerFactory.class));

        TokenFilterFactory filterFactory = analysisService.tokenFilter("icu_normalizer");
        assertThat(filterFactory, instanceOf(IcuNormalizerTokenFilterFactory.class));

        filterFactory = analysisService.tokenFilter("icu_folding");
        assertThat(filterFactory, instanceOf(IcuFoldingTokenFilterFactory.class));

        filterFactory = analysisService.tokenFilter("icu_collation");
        assertThat(filterFactory, instanceOf(IcuCollationTokenFilterFactory.class));

        filterFactory = analysisService.tokenFilter("icu_transform");
        assertThat(filterFactory, instanceOf(IcuTransformTokenFilterFactory.class));

        CharFilterFactory charFilterFactory = analysisService.charFilter("icu_normalizer");
        assertThat(charFilterFactory, instanceOf(IcuNormalizerCharFilterFactory.class));
    }
}
