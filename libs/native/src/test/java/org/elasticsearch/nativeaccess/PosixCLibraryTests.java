/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Unwrappable;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;

import static org.elasticsearch.common.Numbers.isPowerOfTwo;
import static org.elasticsearch.nativeaccess.lib.PosixCLibrary.POSIX_MADV_WILLNEED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PosixCLibraryTests extends ESTestCase {
    NativeAccess nativeAccess;
    PosixCLibrary clib;

    @Before
    public void setup() {
        nativeAccess = NativeAccess.instance();
        if (Constants.LINUX || Constants.MAC_OS_X) {
            clib = NativeLibraryProvider.instance().getLibrary(PosixCLibrary.class);
            assertNotNull(clib);
        } else {
            assumeFalse("posix only available on Mac/Linux", Constants.WINDOWS);
        }
    }

    public void testMadvise() throws IOException {
        int size = randomIntBetween(8, 4096);
        var tmp = createTempDir();
        try (var dir = newFSDirectory(tmp)) {
            try (var out = dir.createOutput("foo.dat", IOContext.DEFAULT)) {
                out.writeBytes(randomBytes(size), 0, size);
            }
        }
        // we need to unwrap our test-only file system layers
        var file = Unwrappable.unwrapAll(tmp.resolve("foo.dat"));
        try (var arena = Arena.ofConfined(); var fc = FileChannel.open(file, StandardOpenOption.READ)) {
            var segment = fc.map(MapMode.READ_ONLY, 0, fc.size(), arena);
            long length = randomLongBetween(1, fc.size());
            assertThat(clib.madvise(segment, 0, length, POSIX_MADV_WILLNEED), equalTo(0));
            assertThat(clib.madvise(segment, 0, size, POSIX_MADV_WILLNEED), equalTo(0));

            assertOutOfBounds(segment);
            if (STRICT_ALIGNMENT) assertUnaligned(segment);
        }
    }

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    private void assertOutOfBounds(MemorySegment segment) {
        final long size = segment.byteSize();
        expectThrows(IOOBE, () -> clib.madvise(segment, 0, -1, POSIX_MADV_WILLNEED));
        expectThrows(IOOBE, () -> clib.madvise(segment, -1, size, POSIX_MADV_WILLNEED));
        expectThrows(IOOBE, () -> clib.madvise(segment, 0, size + 1, POSIX_MADV_WILLNEED));
        expectThrows(IOOBE, () -> clib.madvise(segment, 1, size, POSIX_MADV_WILLNEED));
        expectThrows(IOOBE, () -> clib.madvise(segment, randomIntBetween(1, 10), size + randomIntBetween(1, 10), POSIX_MADV_WILLNEED));
    }

    static final int EINVAL = 22;
    static final boolean STRICT_ALIGNMENT = System.getProperty("os.name").startsWith("Linux");

    private void assertUnaligned(MemorySegment segment) {
        final long size = segment.byteSize();
        assertThat(clib.madvise(segment, 1, size - 1, POSIX_MADV_WILLNEED), equalTo(-1));
        assertThat(clib.errno(), equalTo(EINVAL));
        assertThat(clib.strerror(clib.errno()), containsString("Invalid argument"));

        assertThat(clib.madvise(segment, 3, size - 3, POSIX_MADV_WILLNEED), equalTo(-1));
        assertThat(clib.errno(), equalTo(EINVAL));
        assertThat(clib.strerror(clib.errno()), containsString("Invalid argument"));
    }

    public void testMadviseZeroLength() {
        var mem = MemorySegment.ofAddress(0);
        expectThrows(IOOBE, () -> clib.madvise(mem, 0, 10, POSIX_MADV_WILLNEED));
    }

    public void testMadviseIAE() {
        byte[] buf = randomBytes(randomInt(16));
        var mem = MemorySegment.ofArray(buf);
        expectThrows(IllegalArgumentException.class, () -> clib.madvise(mem, 0, buf.length, POSIX_MADV_WILLNEED));
    }

    public void testPageSize() {
        int pageSize = clib.getPageSize();
        assertThat(pageSize, greaterThan(1));
        assertTrue(isPowerOfTwo(pageSize));
    }

    private byte[] randomBytes(int size) {
        byte[] buffer = new byte[size];
        random().nextBytes(buffer);
        return buffer;
    }
}
