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
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.equalTo;

public class PosixCLibraryTests extends ESTestCase {
    NativeAccess nativeAccess;
    PosixCLibrary clib;

    @Before
    public void setup() {
        nativeAccess = NativeAccess.instance();
        clib = nativeAccess.getPosixCLibrary();
        if (Constants.LINUX || Constants.MAC_OS_X) {
            assertNotNull(clib);
        } else {
            assertNull(clib);
            assumeFalse("posix only available on Mac/Linux", Constants.WINDOWS);
        }
    }

    private byte[] randomBytes(int size) {
        byte[] buffer = new byte[size];
        random().nextBytes(buffer);
        return buffer;
    }

    public void test_madvise() throws IOException {
        long size = randomInt(4096);
        var tmp = createTempDir();
        try (var dir = newFSDirectory(tmp)) {
            try (var out = dir.createOutput("foo.dat", IOContext.DEFAULT)) {
                out.writeBytes(randomBytes((int) size), 0, (int) size);
            }
        }
        try (var fc = FileChannel.open(tmp.resolve("foo.dat"), StandardOpenOption.READ)) {
            var map = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            var mem = MemorySegment.ofBuffer(map);
            long length = randomLongBetween(1, fc.size());
            assertThat(clib.madvise(mem.address(), length, PosixCLibrary.POSIX_FADV_WILLNEED), equalTo(0));
        }
    }
}
