/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.CloseableMappedByteBuffer;
import org.elasticsearch.nativeaccess.lib.JavaLibrary;

import java.io.IOException;
import java.nio.channels.FileChannel;

class JdkJavaLibrary implements JavaLibrary {

    @Override
    public CloseableByteBuffer newSharedBuffer(int len) {
        return JdkCloseableByteBuffer.ofShared(len);
    }

    @Override
    public CloseableByteBuffer newConfinedBuffer(int len) {
        return JdkCloseableByteBuffer.ofConfined(len);
    }

    @Override
    public CloseableMappedByteBuffer map(FileChannel fileChannel, FileChannel.MapMode mode, long position, long size) throws IOException {
        return JdkCloseableMappedByteBuffer.ofShared(fileChannel, mode, position, size);
    }
}
