/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.core.Streams;

import java.io.IOException;

public abstract class BytesStream extends StreamOutput {

    @Override
    public void writeWithSizePrefix(Writeable writeable) throws IOException {
        long pos = position();
        seek(pos + Integer.BYTES);
        try (var out = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.noCloseStream(this)))) {
            out.setTransportVersion(getTransportVersion());
            writeable.writeTo(out);
        }
        long newPos = position();
        seek(pos);
        writeInt(Math.toIntExact(newPos - pos - Integer.BYTES));
        seek(newPos);
    }

    public abstract BytesReference bytes();

    public abstract void seek(long position);
}
