/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.allocation.CacheRestoredAllocationDecider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class CacheSnapshotBootstrapTests extends ESTestCase {

    private static final String ATTR_KEY = "node.attr." + CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR;

    public void testApplyCloneBootSettingsDisabledWhenFeatureFlagOff() throws Exception {
        Path dataPath = createTempDir();
        writeSnapshotFile(dataPath, "source-node-1");
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));

        Settings settings = baseSettings(dataPath).put(
            StatelessSharedBlobCacheService.STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING.getKey(),
            false
        ).build();

        assertThat(CacheSnapshotBootstrap.applyCloneBootSettings(settings), equalTo(Settings.EMPTY));
        assertTrue(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
    }

    public void testApplyCloneBootSettingsNoOpWhenSnapshotFileAbsent() throws Exception {
        Path dataPath = createTempDir();
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));

        Settings settings = baseSettings(dataPath).build();

        assertThat(CacheSnapshotBootstrap.applyCloneBootSettings(settings), equalTo(Settings.EMPTY));
        assertTrue(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
    }

    public void testApplyCloneBootSettingsResetsIdentityAndSetsAttribute() throws Exception {
        Path dataPath = createTempDir();
        writeSnapshotFile(dataPath, "source-node-abc");
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));
        Path legacyMetadata = dataPath.resolve("node-legacy.json");
        Files.writeString(legacyMetadata, "{}");

        Settings settings = baseSettings(dataPath).build();
        Settings bootSettings = CacheSnapshotBootstrap.applyCloneBootSettings(settings);

        assertThat(bootSettings.get(ATTR_KEY), equalTo("source-node-abc"));
        assertFalse(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
        assertFalse(Files.exists(legacyMetadata));
    }

    public void testApplyCloneBootSettingsDoesNotOverrideExplicitAttribute() throws Exception {
        Path dataPath = createTempDir();
        writeSnapshotFile(dataPath, "source-node-abc");
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));

        Settings settings = baseSettings(dataPath).put(ATTR_KEY, "operator-set-node").build();

        assertThat(CacheSnapshotBootstrap.applyCloneBootSettings(settings), equalTo(Settings.EMPTY));
        assertFalse(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
    }

    public void testApplyCloneBootSettingsIgnoresMalformedSnapshotFile() throws Exception {
        Path dataPath = createTempDir();
        Path snapshotFile = CacheSnapshotService.snapshotFilePath(dataPath.resolve(CacheSnapshotBootstrap.SHARED_CACHE_FILE_NAME));
        Files.writeString(snapshotFile, "{not-json");
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));

        Settings settings = baseSettings(dataPath).build();

        assertThat(CacheSnapshotBootstrap.applyCloneBootSettings(settings), equalTo(Settings.EMPTY));
        assertTrue(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
    }

    public void testStatelessPluginAdditionalSettingsInjectsAttributeFromSnapshot() throws Exception {
        Path dataPath = createTempDir();
        writeSnapshotFile(dataPath, "source-from-plugin");
        Files.createDirectory(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME));

        StatelessPlugin plugin = new TestUtils.StatelessPluginWithTrialLicense(
            baseSettings(dataPath).put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build()
        );

        Settings additional = plugin.additionalSettings();
        assertThat(additional.get(ATTR_KEY), equalTo("source-from-plugin"));
        assertFalse(Files.exists(dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME)));
    }

    public void testReadSourceNodeIdRoundTrip() throws Exception {
        Path dataPath = createTempDir();
        writeSnapshotFile(dataPath, "node-read-test");

        Path snapshotFile = CacheSnapshotService.snapshotFilePath(dataPath.resolve(CacheSnapshotBootstrap.SHARED_CACHE_FILE_NAME));
        assertThat(CacheSnapshotService.readSourceNodeId(snapshotFile), equalTo(Optional.of("node-read-test")));
    }

    public void testReadSourceNodeIdMissingFile() {
        assertThat(CacheSnapshotService.readSourceNodeId(createTempDir().resolve("missing.snapshot")), equalTo(Optional.empty()));
    }

    private static Settings.Builder baseSettings(Path dataPath) {
        return Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString())
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName());
    }

    private void writeSnapshotFile(Path dataPath, String sourceNodeId) throws Exception {
        Path snapshotFile = CacheSnapshotService.snapshotFilePath(dataPath.resolve(CacheSnapshotBootstrap.SHARED_CACHE_FILE_NAME));
        Files.createDirectories(snapshotFile.getParent());
        CacheSnapshotService service = new CacheSnapshotService(snapshotFile, () -> "noop");
        service.writeSnapshotFile(sourceNodeId, List.of(), 4, 4096);
    }
}
