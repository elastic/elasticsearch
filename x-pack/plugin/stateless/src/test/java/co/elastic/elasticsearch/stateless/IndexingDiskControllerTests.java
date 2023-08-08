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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.tests.mockfile.FilterFileStore;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Objects;

import static co.elastic.elasticsearch.stateless.IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING;
import static org.elasticsearch.indices.IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingDiskControllerTests extends ESTestCase {

    public void testDefaultRelativeReservedDiskSpace() {
        assertReservedBytes(Settings.EMPTY, ByteSizeValue.ofGb(100L), equalTo(ByteSizeValue.ofGb(20L)));
        assertReservedBytes(Settings.EMPTY, ByteSizeValue.ofGb(120L), equalTo(ByteSizeValue.ofGb(24L)));
        assertReservedBytes(Settings.EMPTY, ByteSizeValue.ofGb(1900L), equalTo(ByteSizeValue.ofGb(380L)));
    }

    public void testRelativeReservedDiskSpace() {
        assertReservedBytes("10%", ByteSizeValue.ofGb(1L), ByteSizeValue.ofGb(100L), equalTo(ByteSizeValue.ofGb(10L)));
        assertReservedBytes("0.5", ByteSizeValue.ofGb(1L), ByteSizeValue.ofGb(120L), equalTo(ByteSizeValue.ofGb(60L)));
    }

    public void testReservedBytesDoesNotExceedTotalSpace() {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> assertReservedBytes("110gb", ByteSizeValue.ofGb(1L), ByteSizeValue.ofGb(100L), new ThrowingAssertionErrorMatcher())
        );
        assertThat(
            exception.getMessage(),
            equalTo("Reserved disk space [110gb (118111600640 bytes)] exceeds total disk space [100gb (107374182400 bytes)]")
        );
    }

    public void testReservedBytesDoesNotExceedLuceneIndexingBuffer() {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> assertReservedBytes(
                ByteSizeValue.ofKb(1L).getStringRep(),
                ByteSizeValue.ofKb(10L),
                ByteSizeValue.ofKb(100L),
                new ThrowingAssertionErrorMatcher()
            )
        );
        assertThat(
            exception.getMessage(),
            equalTo("Reserved disk space [1kb (1024 bytes)] must be larger than Lucene indexing buffer [10kb (10240 bytes)]")
        );
    }

    private void assertReservedBytes(
        String indexingDiskReservedBytesAsString,
        ByteSizeValue indexBufferSize,
        ByteSizeValue totalSpace,
        Matcher<ByteSizeValue> reservedBytesMatcher
    ) {
        assertReservedBytes(
            Settings.builder()
                .put(INDEXING_DISK_RESERVED_BYTES_SETTING.getKey(), indexingDiskReservedBytesAsString)
                .put(INDEX_BUFFER_SIZE_SETTING.getKey(), indexBufferSize)
                .build(),
            totalSpace,
            reservedBytesMatcher
        );
    }

    private void assertReservedBytes(Settings settings, ByteSizeValue totalSpace, Matcher<ByteSizeValue> reservedBytesMatcher) {
        final var fileSystemProvider = new FixedTotalSpaceFileSystemProvider(PathUtils.getDefaultFileSystem(), createTempDir(), totalSpace);
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
        try {
            var finalSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), fileSystemProvider.rootDir.toAbsolutePath().toString())
                .put(settings)
                .build();

            final var indicesService = mock(IndicesService.class);
            when(indicesService.getTotalIndexingBufferBytes()).thenReturn(INDEX_BUFFER_SIZE_SETTING.get(finalSettings));

            try (var nodeEnvironment = new NodeEnvironment(finalSettings, TestEnvironment.newEnvironment(finalSettings))) {
                var indexingDiskController = new IndexingDiskController(
                    nodeEnvironment,
                    finalSettings,
                    mock(ThreadPool.class),
                    indicesService,
                    mock(StatelessCommitService.class)
                );
                assertThat(indexingDiskController.getReservedBytes(), reservedBytesMatcher);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            PathUtilsForTesting.teardown();
        }
    }

    private static class FixedTotalSpaceFileSystemProvider extends FilterFileSystemProvider {

        private final ByteSizeValue totalSpace;
        private final Path rootDir;

        FixedTotalSpaceFileSystemProvider(FileSystem delegateInstance, Path rootDir, ByteSizeValue totalSpace) {
            super("totalspace://", delegateInstance);
            this.totalSpace = Objects.requireNonNull(totalSpace);
            this.rootDir = new FilterPath(rootDir, fileSystem);
        }

        @Override
        public FileStore getFileStore(Path path) {
            try {
                return new FilterFileStore(super.getFileStore(path), getScheme()) {
                    @Override
                    public long getTotalSpace() {
                        return totalSpace.getBytes();
                    }
                };
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static class ThrowingAssertionErrorMatcher extends BaseMatcher<ByteSizeValue> {

        @Override
        public boolean matches(Object actual) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void describeTo(Description description) {
            throw new AssertionError("Should not be called");
        }
    }
}
