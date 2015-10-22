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
package org.elasticsearch.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class IndexSettingsTests extends ESTestCase {

    public void testRunListener() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        Consumer<Settings> settingsConsumer = (s) -> integer.set(s.getAsInt("index.test.setting.int", -1));
        IndexSettings settings = new IndexSettings(new Index("index"), theSettings, Collections.singleton(settingsConsumer));
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertEquals(1, settings.getUpdateListeners().size());
        assertFalse(settings.updateIndexSettings(theSettings));
        assertSame(theSettings, settings.getSettings());
        assertEquals(0, integer.get());
        assertTrue(settings.updateIndexSettings(Settings.builder().put(theSettings).put("index.test.setting.int", 42).build()));
        assertEquals(42, integer.get());
    }

    public void testMergedSettingsArePassed() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        Consumer<Settings> settingsConsumer = (s) -> {
            integer.set(s.getAsInt("index.test.setting.int", -1));
            builder.append(s.get("not.updated", ""));
        };
        IndexSettings settings = new IndexSettings(new Index("index"), theSettings, Collections.singleton(settingsConsumer));
        assertEquals(0, integer.get());
        assertEquals("", builder.toString());
        assertTrue(settings.updateIndexSettings(Settings.builder().put(theSettings).put("index.test.setting.int", 42).build()));
        assertEquals(42, integer.get());
        assertEquals("", builder.toString());
        integer.set(0);
        assertTrue(settings.updateIndexSettings(Settings.builder().put(theSettings).put("not.updated", "boom").build()));
        assertEquals("boom", builder.toString());
        assertEquals(42, integer.get());

    }

    public void testListenerCanThrowException() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        Consumer<Settings> settingsConsumer = (s) -> integer.set(s.getAsInt("index.test.setting.int", -1));
        Consumer<Settings> exceptionConsumer = (s) -> {throw new RuntimeException("boom");};
        List<Consumer<Settings>> list = new ArrayList<>();
        list.add(settingsConsumer);
        list.add(exceptionConsumer);
        Collections.shuffle(list, random());
        IndexSettings settings = new IndexSettings(new Index("index"), theSettings, list);
        assertEquals(0, integer.get());
        assertTrue(settings.updateIndexSettings(Settings.builder().put(theSettings).put("index.test.setting.int", 42).build()));
        assertEquals(42, integer.get());
    }

    public void testSettingsConsistency() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        IndexSettings settings = new IndexSettings(new Index("index"), theSettings, Collections.EMPTY_LIST);
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        try {
            settings.updateIndexSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build());
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("version mismatch on settings update expected: "));
        }

        theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        settings = new IndexSettings(new Index("index"), theSettings, Collections.EMPTY_LIST);
        try {
            settings.updateIndexSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build());
            fail("uuid missing/change");
        } catch (IllegalArgumentException ex) {
            assertEquals("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_", ex.getMessage());
        }
        assertSame(theSettings, settings.getSettings());
    }


}
