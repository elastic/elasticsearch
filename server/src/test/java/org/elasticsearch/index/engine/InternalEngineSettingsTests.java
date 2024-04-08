/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.LiveIndexWriterConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.EngineAccess;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class InternalEngineSettingsTests extends ESSingleNodeTestCase {

    public void testSettingsUpdate() {
        final IndexService service = createIndex("foo");
        InternalEngine engine = ((InternalEngine) EngineAccess.engine(service.getShardOrNull(0)));
        assertThat(engine.getCurrentIndexWriterConfig().getUseCompoundFile(), is(true));
        final int iters = between(1, 20);
        for (int i = 0; i < iters; i++) {

            // Tricky: TimeValue.parseTimeValue casts this long to a double, which steals 11 of the 64 bits for exponent, so we can't use
            // the full long range here else the assert below fails:
            long gcDeletes = random().nextLong() & (Long.MAX_VALUE >> 11);

            Settings build = Settings.builder()
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeletes, TimeUnit.MILLISECONDS)
                .build();
            assertEquals(gcDeletes, build.getAsTime(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), null).millis());

            indicesAdmin().prepareUpdateSettings("foo").setSettings(build).get();
            LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();
            assertEquals(currentIndexWriterConfig.getUseCompoundFile(), true);

            assertEquals(engine.config().getIndexSettings().getGcDeletesInMillis(), gcDeletes);
            assertEquals(engine.getGcDeletesInMillis(), gcDeletes);

        }

        Settings settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), 1000, TimeUnit.MILLISECONDS).build();
        indicesAdmin().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "0ms").build();

        indicesAdmin().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 0);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), 1000, TimeUnit.MILLISECONDS).build();
        indicesAdmin().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());
    }
}
