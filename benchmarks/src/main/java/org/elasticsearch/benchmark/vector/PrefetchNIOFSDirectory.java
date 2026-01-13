/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.nio.file.Path;

public class PrefetchNIOFSDirectory extends XNIOFSDirectory {
    public static final int POSIX_FADV_NORMAL     = 0;
    public static final int POSIX_FADV_RANDOM     = 1;
    public static final int POSIX_FADV_SEQUENTIAL = 2;
    public static final int POSIX_FADV_WILLNEED   = 3;
    public static final int POSIX_FADV_DONTNEED   = 4;
    public static final int POSIX_FADV_NOREUSE    = 5;

    private static NativeAccess NATIVE_ACCESS = NativeAccess.instance();

    private final Path path;

    public PrefetchNIOFSDirectory(Path path) throws IOException {
        super(path);
        this.path = path;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String fileName = path.resolve(name).toString();
        int fd = NATIVE_ACCESS.open(fileName, 0);
        System.out.println("Open " + fileName + " " + fd);
        var in = super.openInput(name, context);
        return new PrefetchIndexInput("", fd, in);
    }

    static class PrefetchIndexInput extends FilterIndexInput {
        final int fd;

        /**
         * Creates a FilterIndexInput with a resource description and wrapped delegate IndexInput
         *
         * @param resourceDescription
         * @param in
         */
        PrefetchIndexInput(String resourceDescription, int fd, IndexInput in) {
            super(resourceDescription, in);
            this.fd = fd;
        }

        @Override
        public void readFloats(float[] floats, int offset, int len) throws IOException {
            in.readFloats(floats, offset, len);
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            NATIVE_ACCESS.fadvise(fd, offset, length, POSIX_FADV_WILLNEED);
        }

        @Override
        public void close() throws IOException {
            in.close();
            NATIVE_ACCESS.close(fd);
        }

        @Override
        public IndexInput clone() {
            return new PrefetchIndexInput("clone", fd, in.clone());
        }
    }
}
