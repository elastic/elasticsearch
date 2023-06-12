/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
