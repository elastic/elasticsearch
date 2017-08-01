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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Collections;

public class MetaDataIndexUpgradeServiceTests extends ESTestCase {

    public void testArchiveBrokenIndexSettings() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.emptyList());
        IndexMetaData src = newIndexMeta("foo", Settings.EMPTY);
        IndexMetaData indexMetaData = service.archiveBrokenIndexSettings(src);
        assertSame(indexMetaData, src);

        src = newIndexMeta("foo", Settings.builder().put("index.refresh_interval", "-200").build());
        indexMetaData = service.archiveBrokenIndexSettings(src);
        assertNotSame(indexMetaData, src);
        assertEquals("-200", indexMetaData.getSettings().get("archived.index.refresh_interval"));

        src = newIndexMeta("foo", Settings.builder().put("index.codec", "best_compression1").build());
        indexMetaData = service.archiveBrokenIndexSettings(src);
        assertNotSame(indexMetaData, src);
        assertEquals("best_compression1", indexMetaData.getSettings().get("archived.index.codec"));

        src = newIndexMeta("foo", Settings.builder().put("index.refresh.interval", "-1").build());
        indexMetaData = service.archiveBrokenIndexSettings(src);
        assertNotSame(indexMetaData, src);
        assertEquals("-1", indexMetaData.getSettings().get("archived.index.refresh.interval"));

        src = newIndexMeta("foo", indexMetaData.getSettings()); // double archive?
        indexMetaData = service.archiveBrokenIndexSettings(src);
        assertSame(indexMetaData, src);
    }

    public void testUpgrade() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.emptyList());
        IndexMetaData src = newIndexMeta("foo", Settings.builder().put("index.refresh_interval", "-200").build());
        assertFalse(service.isUpgraded(src));
        src = service.upgradeIndexMetaData(src, Version.CURRENT.minimumIndexCompatibilityVersion());
        assertTrue(service.isUpgraded(src));
        assertEquals("-200", src.getSettings().get("archived.index.refresh_interval"));
        assertNull(src.getSettings().get("index.refresh_interval"));
        assertSame(src, service.upgradeIndexMetaData(src, Version.CURRENT.minimumIndexCompatibilityVersion())); // no double upgrade
    }

    public void testIsUpgraded() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.emptyList());
        IndexMetaData src = newIndexMeta("foo", Settings.builder().put("index.refresh_interval", "-200").build());
        assertFalse(service.isUpgraded(src));
        Version version = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getPreviousVersion());
        src = newIndexMeta("foo", Settings.builder().put(IndexMetaData.SETTING_VERSION_UPGRADED, version).build());
        assertFalse(service.isUpgraded(src));
        src = newIndexMeta("foo", Settings.builder().put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.CURRENT).build());
        assertTrue(service.isUpgraded(src));
    }

    public void testFailUpgrade() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.emptyList());
        final IndexMetaData metaData = newIndexMeta("foo", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.V_5_0_0_beta1)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.fromString("2.4.0"))
            .build());
        String message = expectThrows(IllegalStateException.class, () -> service.upgradeIndexMetaData(metaData,
            Version.CURRENT.minimumIndexCompatibilityVersion())).getMessage();
        assertEquals(message, "The index [[foo/BOOM]] was created with version [2.4.0] but the minimum compatible version is [5.0.0]." +
            " It should be re-indexed in Elasticsearch 5.x before upgrading to " + Version.CURRENT.toString() + ".");

        IndexMetaData goodMeta = newIndexMeta("foo", Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.V_5_0_0_beta1)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.fromString("5.1.0"))
            .build());
        service.upgradeIndexMetaData(goodMeta, Version.V_5_0_0.minimumIndexCompatibilityVersion());
    }

    public void testPluginUpgrade() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.singletonList(
                indexMetaData -> IndexMetaData.builder(indexMetaData)
                    .settings(
                        Settings.builder()
                            .put(indexMetaData.getSettings())
                            .put("index.refresh_interval", "10s")
                    ).build()));
        IndexMetaData src = newIndexMeta("foo", Settings.builder().put("index.refresh_interval", "200s").build());
        assertFalse(service.isUpgraded(src));
        src = service.upgradeIndexMetaData(src, Version.CURRENT.minimumIndexCompatibilityVersion());
        assertTrue(service.isUpgraded(src));
        assertEquals("10s", src.getSettings().get("index.refresh_interval"));
        assertSame(src, service.upgradeIndexMetaData(src, Version.CURRENT.minimumIndexCompatibilityVersion())); // no double upgrade
    }

    public void testPluginUpgradeFailure() {
        MetaDataIndexUpgradeService service = new MetaDataIndexUpgradeService(Settings.EMPTY, xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            Collections.singletonList(
                indexMetaData -> {
                    throw new IllegalStateException("Cannot upgrade index " + indexMetaData.getIndex().getName());
                }
            ));
        IndexMetaData src = newIndexMeta("foo", Settings.EMPTY);
        String message = expectThrows(IllegalStateException.class, () -> service.upgradeIndexMetaData(src,
            Version.CURRENT.minimumIndexCompatibilityVersion())).getMessage();
        assertEquals(message, "Cannot upgrade index foo");
    }

    public static IndexMetaData newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_CREATION_DATE, 1)
            .put(IndexMetaData.SETTING_INDEX_UUID, "BOOM")
            .put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.V_5_0_0_beta1)
            .put(indexSettings)
            .build();
        IndexMetaData metaData = IndexMetaData.builder(name).settings(build).build();
        return metaData;
    }
}
