/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import java.nio.ByteBuffer;

public non-sealed interface ZstdLibrary extends NativeLibrary {

    long compressBound(int scrLen);

    long compress(ByteBuffer dst, ByteBuffer src, int compressionLevel);

    boolean isError(long code);

    String getErrorName(long code);

    long decompress(ByteBuffer dst, ByteBuffer src);
}
