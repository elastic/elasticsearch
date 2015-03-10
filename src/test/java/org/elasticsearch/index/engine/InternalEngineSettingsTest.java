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
package org.elasticsearch.index.engine;

import org.apache.lucene.index.LiveIndexWriterConfig;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InternalEngineSettingsTest extends ElasticsearchSingleNodeTest {

    public void testSettingsUpdate() {
        final IndexService service = createIndex("foo");
        // INDEX_COMPOUND_ON_FLUSH
        InternalEngine engine = ((InternalEngine)engine(service));
        assertThat(engine.getCurrentIndexWriterConfig().getUseCompoundFile(), is(true));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, false).build()).get();
        assertThat(engine.getCurrentIndexWriterConfig().getUseCompoundFile(), is(false));
        client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, true).build()).get();
        assertThat(engine.getCurrentIndexWriterConfig().getUseCompoundFile(), is(true));


        // VERSION MAP SIZE
        long indexBufferSize = engine.config().getIndexingBufferSize().bytes();
        long versionMapSize = engine.config().getVersionMapSize().bytes();
        assertThat(versionMapSize, equalTo((long) (indexBufferSize * 0.25)));

        final int iters = between(1, 20);
        for (int i = 0; i < iters; i++) {
            boolean compoundOnFlush = randomBoolean();
            long gcDeletes = Math.max(0, randomLong());
            boolean versionMapAsPercent = randomBoolean();
            double versionMapPercent = randomIntBetween(0, 100);
            long versionMapSizeInMB = randomIntBetween(10, 20);
            String versionMapString = versionMapAsPercent ? versionMapPercent + "%" : versionMapSizeInMB + "mb";

            Settings build = ImmutableSettings.builder()
                    .put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, compoundOnFlush)
                    .put(EngineConfig.INDEX_GC_DELETES_SETTING, gcDeletes)
                    .put(EngineConfig.INDEX_VERSION_MAP_SIZE, versionMapString)
                    .build();

            client().admin().indices().prepareUpdateSettings("foo").setSettings(build).get();
            LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();
            assertEquals(engine.config().isCompoundOnFlush(), compoundOnFlush);
            assertEquals(currentIndexWriterConfig.getUseCompoundFile(), compoundOnFlush);


            assertEquals(engine.config().getGcDeletesInMillis(), gcDeletes);
            assertEquals(engine.getGcDeletesInMillis(), gcDeletes);

            indexBufferSize = engine.config().getIndexingBufferSize().bytes();
            versionMapSize = engine.config().getVersionMapSize().bytes();
            if (versionMapAsPercent) {
                assertThat(versionMapSize, equalTo((long) (indexBufferSize * (versionMapPercent / 100))));
            } else {
                assertThat(versionMapSize, equalTo(1024 * 1024 * versionMapSizeInMB));
            }
        }

        Settings settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_GC_DELETES_SETTING, 1000)
                .build();
        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());


        settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_GC_DELETES_SETTING, "0ms")
                .build();

        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 0);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_GC_DELETES_SETTING, 1000)
                .build();
        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_VERSION_MAP_SIZE, "sdfasfd")
                .build();
        try {
            client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
            fail("settings update didn't fail, but should have");
        } catch (ElasticsearchIllegalArgumentException e) {
            // good
        }

        settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_VERSION_MAP_SIZE, "-12%")
                .build();
        try {
            client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
            fail("settings update didn't fail, but should have");
        } catch (ElasticsearchIllegalArgumentException e) {
            // good
        }

        settings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_VERSION_MAP_SIZE, "130%")
                .build();
        try {
            client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
            fail("settings update didn't fail, but should have");
        } catch (ElasticsearchIllegalArgumentException e) {
            // good
        }
    }


}
