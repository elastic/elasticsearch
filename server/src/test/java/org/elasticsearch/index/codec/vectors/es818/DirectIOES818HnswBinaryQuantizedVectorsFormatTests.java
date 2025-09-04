/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.OptionalLong;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

public class DirectIOES818HnswBinaryQuantizedVectorsFormatTests extends ES818HnswBinaryQuantizedVectorsFormatTests {

    static final Codec codec = TestUtil.alwaysKnnVectorsFormat(
        new DirectIOES818HnswBinaryQuantizedVectorsFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH)
    );

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @BeforeClass
    public static void checkDirectIOSupport() {
        Path path = createTempDir("directIOProbe");
        try (Directory dir = open(path); IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)) {
            out.writeString("test");
        } catch (IOException e) {
            assumeNoException("test requires a filesystem that supports Direct IO", e);
        }
    }

    static DirectIODirectory open(Path path) throws IOException {
        return new DirectIODirectory(FSDirectory.open(path)) {
            @Override
            protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
                return true;
            }
        };
    }

    @Override
    public void testSimpleOffHeapSize() throws IOException {
        var config = newIndexWriterConfig().setUseCompoundFile(false); // avoid compound files to allow directIO
        try (Directory dir = newFSDirectory()) {
            testSimpleOffHeapSizeImpl(dir, config, false);
        }
    }

    private Directory newFSDirectory() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(idxSettings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(idxSettings.getIndex(), 0));
        Directory dir = (new FsDirectoryFactory()).newDirectory(idxSettings, path);
        if (random().nextBoolean()) {
            dir = new MockDirectoryWrapper(random(), dir);
        }
        return dir;
    }
}
