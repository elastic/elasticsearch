/*
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

package org.elasticsearch.core.internal.io;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class IOUtilsTests extends ESTestCase {

    public void testCloseArray() throws IOException {
        runTestClose(Function.identity(), IOUtils::close);
    }

    public void testCloseIterable() throws IOException {
        runTestClose(Arrays::asList, IOUtils::close);
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
        runTestCloseWithIOExceptions(Arrays::asList, IOUtils::close);
    }

    private <T> void runTestCloseWithIOExceptions(
            final Function<Closeable[], T> function, final CheckedConsumer<T, IOException> close) throws IOException {
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

    public void testDeleteFilesIgnoringExceptionsIterable() throws IOException {
        runDeleteFilesIgnoringExceptionsTest(Arrays::asList, IOUtils::deleteFilesIgnoringExceptions);
    }

    private <T> void runDeleteFilesIgnoringExceptionsTest(
            final Function<Path[], T> function, CheckedConsumer<T, IOException> deleteFilesIgnoringExceptions) throws IOException {
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
        final int numberOfLocations = randomIntBetween(0, 7);
        final Path[] locations = new Path[numberOfLocations];
        for (int i = 0; i < numberOfLocations; i++) {
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

        IOUtils.rm(locations);

        for (int i = 0; i < numberOfLocations; i++) {
            assertFalse(locations[i].toString(), Files.exists(locations[i]));
        }
    }

    public void testFsyncDirectory() throws Exception {
        final Path path = createTempDir().toRealPath();
        final Path subPath = path.resolve(randomAlphaOfLength(8));
        Files.createDirectories(subPath);
        IOUtils.fsync(subPath, true);
        // no exception
    }

    public void testFsyncFile() throws IOException {
        final Path path = createTempDir().toRealPath();
        final Path subPath = path.resolve(randomAlphaOfLength(8));
        Files.createDirectories(subPath);
        final Path file = subPath.resolve(randomAlphaOfLength(8));
        try (OutputStream o = Files.newOutputStream(file)) {
            o.write("0\n".getBytes(StandardCharsets.US_ASCII));
        }
        IOUtils.fsync(file, false);
        // no exception
    }

}
