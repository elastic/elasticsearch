/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store.smb;

import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.instanceOf;

/** The SMB store types wrap their directory whichever way the shard asks for it. */
public class SmbDirectoryFactoryTests extends ESTestCase {

    public void testMmapDirectoryIsWrapped() throws IOException {
        assertWrapped(new SmbMmapFsDirectoryFactory());
    }

    public void testNIOFSDirectoryIsWrapped() throws IOException {
        assertWrapped(new SmbNIOFSDirectoryFactory());
    }

    private void assertWrapped(FsDirectoryFactory factory) throws IOException {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "foo",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
        );
        ShardId shardId = new ShardId(indexSettings.getIndex(), 0);
        Path path = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, path, path, shardId);
        try (Directory directory = factory.newDirectory(indexSettings, shardPath)) {
            assertThat(directory, instanceOf(SmbDirectoryWrapper.class));
        }
        try (Directory directory = factory.newDirectory(indexSettings, shardPath, null, () -> MappingLookup.EMPTY)) {
            assertThat(directory, instanceOf(SmbDirectoryWrapper.class));
        }
    }
}
