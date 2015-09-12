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
import org.elasticsearch.indices.analysis.PreBuiltTokenizers;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

/**
 *
 */
public class PreBuiltTokenizerFactoryFactoryTests extends ESTestCase {

    @Test
    public void testThatDifferentVersionsCanBeLoaded() {
        PreBuiltTokenizerFactoryFactory factory = new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.STANDARD.getTokenizerFactory(Version.CURRENT));

        // different es versions, same lucene version, thus cached
        TokenizerFactory former090TokenizerFactory = factory.create("standard", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_1).build());
        TokenizerFactory former090TokenizerFactoryCopy = factory.create("standard", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_2).build());
        TokenizerFactory currentTokenizerFactory = factory.create("standard", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());

        assertThat(currentTokenizerFactory, is(not(former090TokenizerFactory)));
        assertThat(currentTokenizerFactory, is(not(former090TokenizerFactoryCopy)));
        assertThat(former090TokenizerFactory, is(former090TokenizerFactoryCopy));
    }

}
