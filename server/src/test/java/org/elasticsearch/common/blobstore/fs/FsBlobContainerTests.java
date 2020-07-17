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
package org.elasticsearch.common.blobstore.fs;

import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterSeekableByteChannel;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public class FsBlobContainerTests extends ESTestCase {

    final AtomicLong totalBytesRead = new AtomicLong(0);
    FileSystem fileSystem = null;

    @Before
    public void setupMockFileSystems() {
        FileSystemProvider fileSystemProvider = new MockFileSystemProvider(PathUtils.getDefaultFileSystem(), totalBytesRead::addAndGet);
        fileSystem = fileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem); // restored by restoreFileSystem in ESTestCase
    }

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystem);
    }

    public void testReadBlobRangeCorrectlySkipBytes() throws IOException {
        final String blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final byte[] blobData = randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb

        final Path path = PathUtils.get(createTempDir().toString());
        Files.write(path.resolve(blobName), blobData);

        final FsBlobContainer container = new FsBlobContainer(new FsBlobStore(Settings.EMPTY, path, false), BlobPath.cleanPath(), path);
        assertThat(totalBytesRead.get(), equalTo(0L));

        final long start = randomLongBetween(0L, Math.max(0L, blobData.length - 1));
        final long length = randomLongBetween(1L, blobData.length - start);

        try (InputStream stream = container.readBlob(blobName, start, length)) {
            assertThat(totalBytesRead.get(), equalTo(0L));
            assertThat(Streams.consumeFully(stream), equalTo(length));
            assertThat(totalBytesRead.get(), equalTo(length));
        }
    }

    public void testTempBlobName() {
        final String blobName = randomAlphaOfLengthBetween(1, 20);
        final String tempBlobName = FsBlobContainer.tempBlobName(blobName);
        assertThat(tempBlobName, startsWith("pending-"));
        assertThat(tempBlobName, containsString(blobName));
    }

    public void testIsTempBlobName() {
        final String tempBlobName = FsBlobContainer.tempBlobName(randomAlphaOfLengthBetween(1, 20));
        assertThat(FsBlobContainer.isTempBlobName(tempBlobName), is(true));
    }

    static class MockFileSystemProvider extends FilterFileSystemProvider {

        final Consumer<Long> onRead;

        MockFileSystemProvider(FileSystem inner, Consumer<Long> onRead) {
            super("mockfs://", inner);
            this.onRead = onRead;
        }

        private int onRead(int read) {
            if (read != -1) {
                onRead.accept((long) read);
            }
            return read;
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> opts, FileAttribute<?>... attrs) throws IOException {
            return new FilterSeekableByteChannel(super.newByteChannel(path, opts, attrs)) {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return onRead(super.read(dst));
                }
            };
        }

        @Override
        public InputStream newInputStream(Path path, OpenOption... opts) throws IOException {
            // no super.newInputStream(path, opts) as it will use the delegating FileSystem to open a SeekableByteChannel
            // and instead we want the mocked newByteChannel() method to be used
            return new FilterInputStream(delegate.newInputStream(path, opts)) {
                @Override
                public int read() throws IOException {
                    return onRead(super.read());
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return onRead(super.read(b, off, len));
                }
            };
        }
    }
}
