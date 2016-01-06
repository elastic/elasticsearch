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
import org.elasticsearch.index.translog.Translog;
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
        IndexMetaData metaData = newIndexMeta("index", theSettings);
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY, Collections.singleton(settingsConsumer));
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertEquals(1, settings.getUpdateListeners().size());
        assertFalse(settings.updateIndexMetaData(metaData));
        assertEquals(metaData.getSettings().getAsMap(), settings.getSettings().getAsMap());
        assertEquals(0, integer.get());
        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())));
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
            builder.append(s.get("index.not.updated", ""));
        };
        IndexSettings settings = new IndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, Collections.singleton(settingsConsumer));
        assertEquals(0, integer.get());
        assertEquals("", builder.toString());
        IndexMetaData newMetaData = newIndexMeta("index", Settings.builder().put(settings.getIndexMetaData().getSettings()).put("index.test.setting.int", 42).build());
        assertTrue(settings.updateIndexMetaData(newMetaData));
        assertSame(settings.getIndexMetaData(), newMetaData);
        assertEquals(42, integer.get());
        assertEquals("", builder.toString());
        integer.set(0);
        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(settings.getIndexMetaData().getSettings()).put("index.not.updated", "boom").build())));
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
        IndexSettings settings = new IndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, list);
        assertEquals(0, integer.get());
        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())));
        assertEquals(42, integer.get());
    }

    public void testSettingsConsistency() {
        Version version = VersionUtils.getPreviousVersion();
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY, Collections.emptyList());
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()));
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("version mismatch on settings update expected: "));
        }

        metaData = newIndexMeta("index", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build());
        settings = new IndexSettings(metaData, Settings.EMPTY, Collections.emptyList());
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()));
            fail("uuid missing/change");
        } catch (IllegalArgumentException ex) {
            assertEquals("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_", ex.getMessage());
        }
        assertEquals(metaData.getSettings().getAsMap(), settings.getSettings().getAsMap());
    }


    public void testNodeSettingsAreContained() {
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 10);
        Settings theSettings = Settings.settingsBuilder().
                put("index.foo.bar", 42)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards).build();

        Settings nodeSettings = Settings.settingsBuilder().put("node.foo.bar", 43).build();
        final AtomicInteger indexValue = new AtomicInteger(0);
        final AtomicInteger nodeValue = new AtomicInteger(0);
        Consumer<Settings> settingsConsumer = (s) -> {indexValue.set(s.getAsInt("index.foo.bar", -1)); nodeValue.set(s.getAsInt("node.foo.bar", -1));};
        IndexSettings settings = new IndexSettings(newIndexMeta("index", theSettings), nodeSettings, Collections.singleton(settingsConsumer));
        assertEquals(numReplicas, settings.getNumberOfReplicas());
        assertEquals(numShards, settings.getNumberOfShards());
        assertEquals(0, indexValue.get());
        assertEquals(0, nodeValue.get());

        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.settingsBuilder().
                put("index.foo.bar", 42)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards).build())));

        assertEquals(42, indexValue.get());
        assertEquals(43, nodeValue.get());
        assertSame(nodeSettings, settings.getNodeSettings());


    }

    private IndexMetaData newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(indexSettings)
                .build();
        IndexMetaData metaData = IndexMetaData.builder(name).settings(build).build();
        return metaData;
    }


    public void testUpdateDurability() {
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY, "async")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY, Collections.emptyList());
        assertEquals(Translog.Durability.ASYNC, settings.getTranslogDurability());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY, "request").build()));
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability());

        metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY, Collections.emptyList());
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability()); // test default
    }


}
