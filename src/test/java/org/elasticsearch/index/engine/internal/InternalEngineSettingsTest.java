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
package org.elasticsearch.index.engine.internal;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.hamcrest.Matchers.is;

public class InternalEngineSettingsTest extends ElasticsearchSingleNodeTest {

    public void testLuceneSettings() {
        final IndexService service = createIndex("foo");
        // INDEX_COMPOUND_ON_FLUSH
        assertThat(engine(service).currentIndexWriterConfig().getUseCompoundFile(), is(true));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, false).build()).get();
        assertThat(engine(service).currentIndexWriterConfig().getUseCompoundFile(), is(false));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true).build()).get();
        assertThat(engine(service).currentIndexWriterConfig().getUseCompoundFile(), is(true));
        
        // INDEX_CHECKSUM_ON_MERGE
        assertThat(engine(service).currentIndexWriterConfig().getCheckIntegrityAtMerge(), is(true));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_CHECKSUM_ON_MERGE, false).build()).get();
        assertThat(engine(service).currentIndexWriterConfig().getCheckIntegrityAtMerge(), is(false));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_CHECKSUM_ON_MERGE, true).build()).get();
        assertThat(engine(service).currentIndexWriterConfig().getCheckIntegrityAtMerge(), is(true));
    }


}
