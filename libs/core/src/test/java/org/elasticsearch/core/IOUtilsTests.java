/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.core;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class IOUtilsTests extends ESTestCase {

    public void testCloseArray() throws IOException {
        runTestClose(Function.identity(), IOUtils::close);
    }

    public void testCloseIterable() throws IOException {
        runTestClose((Function<Closeable[], List<Closeable>>) Arrays::asList, IOUtils::close);
    }

    private <T> void runTestClose(final Function<Closeable[], T> function, final CheckedConsumer<T, IOException> close) throws IOException {
        final int numberOfCloseables = randomIntBetween(0, 7);
        final Closeable[] closeables = new Closeable[numberOfCloseables];
        for (int i = 0; i < numberOfCloseables; i++) {
            closeables[i] = mock(Closeable.class);
        }
        close.accept(function.apply(closeables));
        for (int i = 0; i < numberOfCloseables; i++) {
            verify(closeables[i]).close();
            verifyNoMoreInteractions(closeables[i]);
        }
    }

    public void testCloseArrayWithIOExceptions() throws IOException {
        runTestCloseWithIOExceptions(Function.identity(), IOUtils::close);
    }

    public void testCloseIterableWithIOExceptions() throws IOException {
        runTestCloseWithIOExceptions((Function<Closeable[], List<Closeable>>) Arrays::asList, IOUtils::close);
    }

    private <T> void runTestCloseWithIOExceptions(final Function<Closeable[], T> function, final CheckedConsumer<T, IOException> close)
        throws IOException {
        final int numberOfCloseables = randomIntBetween(1, 8);
        final Closeable[] closeables = new Closeable[numberOfCloseables];
        final List<Integer> indexesThatThrow = new ArrayList<>(numberOfCloseables);
        for (int i = 0; i < numberOfCloseables - 1; i++) {
            final Closeable closeable = mock(Closeable.class);
            if (randomBoolean()) {
                indexesThatThrow.add(i);
                doThrow(new IOException(Integer.toString(i))).when(closeable).close();
            }
            closeables[i] = closeable;
        }

        // ensure that at least one always throws
        final Closeable closeable = mock(Closeable.class);
        if (indexesThatThrow.isEmpty() || randomBoolean()) {
            indexesThatThrow.add(numberOfCloseables - 1);
            doThrow(new IOException(Integer.toString(numberOfCloseables - 1))).when(closeable).close();
        }
        closeables[numberOfCloseables - 1] = closeable;

        final IOException e = expectThrows(IOException.class, () -> close.accept(function.apply(closeables)));
        assertThat(e.getMessage(), equalTo(Integer.toString(indexesThatThrow.get(0))));
        assertThat(e.getSuppressed(), arrayWithSize(indexesThatThrow.size() - 1));
        for (int i = 1; i < indexesThatThrow.size(); i++) {
            assertNotNull(e.getSuppressed()[i - 1]);
            assertThat(e.getSuppressed()[i - 1].getMessage(), equalTo(Integer.toString(indexesThatThrow.get(i))));
        }
    }

    public void testDeleteFilesIgnoringExceptionsArray() throws IOException {
        runDeleteFilesIgnoringExceptionsTest(Function.identity(), IOUtils::deleteFilesIgnoringExceptions);
    }

    private <T> void runDeleteFilesIgnoringExceptionsTest(
        final Function<Path[], T> function,
        CheckedConsumer<T, IOException> deleteFilesIgnoringExceptions
    ) throws IOException {
        final int numberOfFiles = randomIntBetween(0, 7);
        final Path[] files = new Path[numberOfFiles];
        for (int i = 0; i < numberOfFiles; i++) {
            if (randomBoolean()) {
                files[i] = createTempFile();
            } else {
                final Path temporary = createTempFile();
                files[i] = PathUtils.get(temporary.toString(), randomAlphaOfLength(8));
                Files.delete(temporary);
            }
        }
        deleteFilesIgnoringExceptions.accept(function.apply(files));
        for (int i = 0; i < numberOfFiles; i++) {
            assertFalse(files[i].toString(), Files.exists(files[i]));
        }
    }

    public void testRm() throws IOException {
        runTestRm(false);
    }

    public void testRmWithIOExceptions() throws IOException {
        runTestRm(true);
    }

    public void runTestRm(final boolean exception) throws IOException {
        final int numberOfLocations = randomIntBetween(0, 7);
        final Path[] locations = new Path[numberOfLocations];
        final List<Path> locationsThrowingException = new ArrayList<>(numberOfLocations);
        for (int i = 0; i < numberOfLocations; i++) {
            if (exception && randomBoolean()) {
                final Path location = createTempDir();
                final FilterFileSystemProvider fsProvider = new AccessDeniedWhileDeletingFileSystem(location.getFileSystem());
                final Path wrapped = fsProvider.wrapPath(location);
                locations[i] = wrapped.resolve(randomAlphaOfLength(8));
                Files.createDirectory(locations[i]);
                locationsThrowingException.add(locations[i]);
            } else {
                // we create a tree of files that IOUtils#rm should delete
                locations[i] = createTempDir();
                Path location = locations[i];
                while (true) {
                    location = Files.createDirectory(location.resolve(randomAlphaOfLength(8)));
                    if (rarely() == false) {
                        Files.createTempFile(location, randomAlphaOfLength(8), null);
                        break;
                    }
                }
            }
        }

        if (locationsThrowingException.isEmpty()) {
            IOUtils.rm(locations);
        } else {
            final IOException e = expectThrows(IOException.class, () -> IOUtils.rm(locations));
            assertThat(e, hasToString(containsString("could not remove the following files (in the order of attempts):")));
            for (final Path locationThrowingException : locationsThrowingException) {
                assertThat(e, hasToString(containsString("access denied while trying to delete file [" + locationThrowingException + "]")));
            }
        }

        for (int i = 0; i < numberOfLocations; i++) {
            if (locationsThrowingException.contains(locations[i]) == false) {
                assertFalse(locations[i].toString(), Files.exists(locations[i]));
            }
        }
    }

    private static final class AccessDeniedWhileDeletingFileSystem extends FilterFileSystemProvider {

        /**
         * Create a new instance, wrapping {@code delegate}.
         */
        AccessDeniedWhileDeletingFileSystem(final FileSystem delegate) {
            super("access_denied://", delegate);
        }

        @Override
        public void delete(final Path path) throws IOException {
            if (Files.exists(path)) {
                throw new AccessDeniedException("access denied while trying to delete file [" + path + "]");
            }
            super.delete(path);
        }

    }

    private void fsync(final Path path, final boolean isDir) throws IOException {
        IOUtils.fsync(path, isDir, randomBoolean());
    }

    public void testFsyncDirectory() throws Exception {
        final Path path = createTempDir().toRealPath();
        final Path subPath = path.resolve(randomAlphaOfLength(8));
        Files.createDirectories(subPath);
        fsync(subPath, true);
        // no exception
    }

    private static final class AccessDeniedWhileOpeningDirectoryFileSystem extends FilterFileSystemProvider {

        AccessDeniedWhileOpeningDirectoryFileSystem(final FileSystem delegate) {
            super("access_denied://", Objects.requireNonNull(delegate));
        }

        @Override
        public FileChannel newFileChannel(final Path path, final Set<? extends OpenOption> options, final FileAttribute<?>... attrs)
            throws IOException {
            if (Files.isDirectory(path)) {
                throw new AccessDeniedException(path.toString());
            }
            return delegate.newFileChannel(path, options, attrs);
        }

    }

    public void testFsyncAccessDeniedOpeningDirectory() throws Exception {
        final Path path = createTempDir().toRealPath();
        final FilterFileSystemProvider fsProvider = new AccessDeniedWhileOpeningDirectoryFileSystem(path.getFileSystem());
        final Path wrapped = fsProvider.wrapPath(path);
        if (Constants.WINDOWS) {
            // no exception, we early return and do not even try to open the directory
            fsync(wrapped, true);
        } else {
            expectThrows(AccessDeniedException.class, () -> fsync(wrapped, true));
        }
    }

    public void testFsyncNonExistentDirectory() throws Exception {
        final Path dir = FilterPath.unwrap(createTempDir()).toRealPath();
        final Path nonExistentDir = dir.resolve("non-existent");
        expectThrows(NoSuchFileException.class, () -> fsync(nonExistentDir, true));
    }

    public void testFsyncFile() throws IOException {
        final Path path = createTempDir().toRealPath();
        final Path subPath = path.resolve(randomAlphaOfLength(8));
        Files.createDirectories(subPath);
        final Path file = subPath.resolve(randomAlphaOfLength(8));
        try (OutputStream o = Files.newOutputStream(file)) {
            o.write("0\n".getBytes(StandardCharsets.US_ASCII));
        }
        fsync(file, false);
        // no exception
    }

}
