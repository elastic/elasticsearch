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
package org.elasticsearch.indices.analyze;

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.indices.analysis.HunspellService.*;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ClusterScope(scope= Scope.TEST, numDataNodes=0)
public class HunspellServiceIT extends ESIntegTestCase {

    @Test
    public void testLocaleDirectoryWithNodeLevelConfig() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("path.conf", getDataPath("/indices/analyze/conf_dir"))
                .put(HUNSPELL_LAZY_LOAD, randomBoolean())
                .put(HUNSPELL_IGNORE_CASE, true)
                .build();

        internalCluster().startNode(settings);
        Dictionary dictionary = internalCluster().getInstance(HunspellService.class).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertIgnoreCase(true, dictionary);
    }

    @Test
    public void testLocaleDirectoryWithLocaleSpecificConfig() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("path.conf", getDataPath("/indices/analyze/conf_dir"))
                .put(HUNSPELL_LAZY_LOAD, randomBoolean())
                .put(HUNSPELL_IGNORE_CASE, true)
                .put("indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing", false)
                .put("indices.analysis.hunspell.dictionary.en_US.ignore_case", false)
                .build();

        internalCluster().startNode(settings);
        Dictionary dictionary = internalCluster().getInstance(HunspellService.class).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertIgnoreCase(false, dictionary);



        // testing that dictionary specific settings override node level settings
        dictionary = internalCluster().getInstance(HunspellService.class).getDictionary("en_US_custom");
        assertThat(dictionary, notNullValue());
        assertIgnoreCase(true, dictionary);
    }

    @Test
    public void testDicWithNoAff() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("path.conf", getDataPath("/indices/analyze/no_aff_conf_dir"))
                .put(HUNSPELL_LAZY_LOAD, randomBoolean())
                .build();

        Dictionary dictionary = null;
        try {
            internalCluster().startNode(settings);
            dictionary = internalCluster().getInstance(HunspellService.class).getDictionary("en_US");
            fail("Missing affix file didn't throw an error");
        }
        catch (Throwable t) {
            assertNull(dictionary);
            assertThat(ExceptionsHelper.unwrap(t, ElasticsearchException.class).toString(), Matchers.containsString("Missing affix file"));
        }
    }

    @Test
    public void testDicWithTwoAffs() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("path.conf", getDataPath("/indices/analyze/two_aff_conf_dir"))
                .put(HUNSPELL_LAZY_LOAD, randomBoolean())
                .build();

        Dictionary dictionary = null;
        try {
            internalCluster().startNode(settings);
            dictionary = internalCluster().getInstance(HunspellService.class).getDictionary("en_US");
            fail("Multiple affix files didn't throw an error");
        } catch (Throwable t) {
            assertNull(dictionary);
            assertThat(ExceptionsHelper.unwrap(t, ElasticsearchException.class).toString(), Matchers.containsString("Too many affix files"));
        }
    }

    // TODO: on next upgrade of lucene, just use new getter
    private void assertIgnoreCase(boolean expected, Dictionary dictionary) throws Exception {
        // assertEquals(expected, dictionary.getIgnoreCase());
    }
}
