/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

public class IndexDirectoryTests extends ESTestCase {

    public void testDisablesFsync() throws IOException {
        final FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        final FilterFileSystemProvider provider = new FilterFileSystemProvider("mock://", fileSystem) {
            @Override
            public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
                return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                    @Override
                    public void force(boolean metaData) {
                        throw new AssertionError("fsync should be disabled by the index directory");
                    }
                };
            }
        };
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        final Path path = PathUtils.get(createTempDir().toString());
        try (
            Directory directory = new IndexDirectory(FSDirectory.open(path), null, null);
            IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())
        ) {
            indexWriter.commit();
        }
    }
}
