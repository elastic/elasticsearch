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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.PreBuiltTokenFilters;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

/**
 *
 */
public class PreBuiltTokenFilterFactoryFactoryTests extends ESTestCase {

    @Test
    public void testThatCachingWorksForCachingStrategyOne() {
        PreBuiltTokenFilterFactoryFactory factory = new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.WORD_DELIMITER.getTokenFilterFactory(Version.CURRENT));

        TokenFilterFactory former090TokenizerFactory = factory.create("word_delimiter", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_1).build());
        TokenFilterFactory former090TokenizerFactoryCopy = factory.create("word_delimiter", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_2).build());
        TokenFilterFactory currentTokenizerFactory = factory.create("word_delimiter", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());

        assertThat(currentTokenizerFactory, is(former090TokenizerFactory));
        assertThat(currentTokenizerFactory, is(former090TokenizerFactoryCopy));
    }

    @Test
    public void testThatDifferentVersionsCanBeLoaded() {
        PreBuiltTokenFilterFactoryFactory factory = new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.STOP.getTokenFilterFactory(Version.CURRENT));

        TokenFilterFactory former090TokenizerFactory = factory.create("stop", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_1).build());
        TokenFilterFactory former090TokenizerFactoryCopy = factory.create("stop", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_2).build());
        TokenFilterFactory currentTokenizerFactory = factory.create("stop", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());

        assertThat(currentTokenizerFactory, is(not(former090TokenizerFactory)));
        assertThat(former090TokenizerFactory, is(former090TokenizerFactoryCopy));
    }

}
