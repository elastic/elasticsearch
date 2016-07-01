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

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class LongUninvertingIndexFieldDataTests extends ESSingleNodeTestCase {
    private static Version[] TEST_VERSIONS =
        new Version[] {Version.V_5_0_0_alpha1, Version.V_5_0_0_alpha2, Version.V_5_0_0_alpha3};

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MapperSizePlugin.class);
    }

    public void testEmpty() throws IOException {
        Version version = randomFrom(TEST_VERSIONS);
        IndexService service = createIndex("foo",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version.id).build(),
            "bar", "_size", "enabled=false");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("foo", "bar").setSource("{\"f\":\"content\"}").execute().actionGet();
        }
        client().admin().indices().prepareRefresh("foo").execute().actionGet();

        MappedFieldType ft = service.mapperService().fullName("_size");
        IndexFieldData<?> ift = service.fieldData().getForField(ft);
        assertThat(ift, instanceOf(LongUninvertingIndexFieldData.class));
        try (Engine.Searcher searcher = service.getShard(0).acquireSearcher("test")) {
            for (LeafReaderContext ctx : searcher.reader().leaves()) {
                AtomicFieldData fd = ift.load(ctx);
                assertThat(fd.ramBytesUsed(), equalTo(0L));
                assertThat(ift.load(ctx), not(equalTo(fd)));
            }
        }
    }

    public void testWithData() throws IOException {
        Version version = randomFrom(TEST_VERSIONS);
        IndexService service = createIndex("foo",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version.id).build(),
            "bar", "_size", "enabled=true");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("foo", "bar").setSource("{\"f\":\"content\"}").execute().actionGet();
        }
        client().admin().indices().prepareRefresh("foo").execute().actionGet();

        MappedFieldType ft = service.mapperService().fullName("_size");
        IndexFieldData<?> ift = service.fieldData().getForField(ft);
        assertThat(ift, instanceOf(LongUninvertingIndexFieldData.class));
        try (Engine.Searcher searcher = service.getShard(0).acquireSearcher("test")) {
            for (LeafReaderContext ctx : searcher.reader().leaves()) {
                AtomicFieldData fd = ift.load(ctx);
                assertThat(fd.ramBytesUsed(), greaterThan(0L));
                assertThat(ift.load(ctx), equalTo(fd));
                assertThat(fd, instanceOf(LongUninvertingAtomicFieldData.class));
            }
        }
    }
}
