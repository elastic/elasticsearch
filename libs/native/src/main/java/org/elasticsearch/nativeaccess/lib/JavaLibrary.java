/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.CloseableMappedByteBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public non-sealed interface JavaLibrary extends NativeLibrary {
    CloseableByteBuffer newSharedBuffer(int len);

    CloseableByteBuffer newConfinedBuffer(int len);

    CloseableMappedByteBuffer map(FileChannel fileChannel, MapMode mode, long position, long size) throws IOException;

}
