/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.analyze;

import org.apache.lucene.analysis.hunspell.HunspellDictionary;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes=0)
public class HunspellServiceTests extends ElasticsearchIntegrationTest {

    @Test
    public void testLocaleDirectoryWithNodeLevelConfig() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.conf", getResource("/indices/analyze/conf_dir"))
                .put("indices.analysis.hunspell.dictionary.lazy", true)
                .put("indices.analysis.hunspell.dictionary.ignore_case", true)
                .build();

        cluster().startNode(settings);
        HunspellDictionary dictionary = cluster().getInstance(HunspellService.class).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        Version expectedVersion = Lucene.parseVersion(settings.get("indices.analysis.hunspell.version"), Lucene.ANALYZER_VERSION, logger);
        assertThat(dictionary.getVersion(), equalTo(expectedVersion));
        assertThat(dictionary.isIgnoreCase(), equalTo(true));
    }

    @Test
    public void testLocaleDirectoryWithLocaleSpecificConfig() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.conf", getResource("/indices/analyze/conf_dir"))
                .put("indices.analysis.hunspell.dictionary.lazy", true)
                .put("indices.analysis.hunspell.dictionary.ignore_case", true)
                .put("indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing", false)
                .put("indices.analysis.hunspell.dictionary.en_US.ignore_case", false)
                .build();

        cluster().startNode(settings);
        HunspellDictionary dictionary = cluster().getInstance(HunspellService.class).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        Version expectedVersion = Lucene.parseVersion(settings.get("indices.analysis.hunspell.version"), Lucene.ANALYZER_VERSION, logger);
        assertThat(dictionary.getVersion(), equalTo(expectedVersion));
        assertThat(dictionary.isIgnoreCase(), equalTo(false));


        // testing that dictionary specific settings override node level settings
        dictionary = cluster().getInstance(HunspellService.class).getDictionary("en_US_custom");
        assertThat(dictionary, notNullValue());
        assertThat(dictionary.getVersion(), equalTo(expectedVersion));
        assertThat(dictionary.isIgnoreCase(), equalTo(true));
    }

    @Test
    public void testCustomizeLocaleDirectory() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("indices.analysis.hunspell.dictionary.location", getResource("/indices/analyze/conf_dir/hunspell"))
                .build();

        cluster().startNode(settings);
        HunspellDictionary dictionary = cluster().getInstance(HunspellService.class).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
    }

}
