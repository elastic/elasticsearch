/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

public class BytesReferenceIndexInput extends IndexInput {

    private final BytesReference bytesReference;

    private int filePointer;
    private StreamInput streamInput;

    public BytesReferenceIndexInput(String resourceDescription, BytesReference bytesReference) {
        this(resourceDescription, bytesReference, 0);
    }

    private BytesReferenceIndexInput(String resourceDescription, BytesReference bytesReference, int filePointer) {
        super(resourceDescription);
        this.bytesReference = bytesReference;
        this.filePointer = filePointer;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
        return filePointer;
    }

    private StreamInput getOrOpenStreamInput() throws IOException {
        if (streamInput == null) {
            streamInput = bytesReference.slice(filePointer, bytesReference.length() - filePointer).streamInput();
        }
        return streamInput;
    }

    @Override
    public void seek(long longPos) throws IOException {
        if (longPos < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + longPos);
        } else if (longPos > bytesReference.length()) {
            throw new EOFException("seek past EOF");
        }
        var pos = (int) longPos;
        if (pos < filePointer) {
            streamInput = null;
        } else if (streamInput != null) {
            final var toSkip = pos - filePointer;
            final var skipped = streamInput.skip(toSkip);
            assert skipped == toSkip;
        }
        filePointer = pos;
    }

    @Override
    public long length() {
        return bytesReference.length();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset >= 0L && length >= 0L && offset + length <= bytesReference.length()) {
            return new BytesReferenceIndexInput(sliceDescription, bytesReference.slice((int) offset, (int) length));
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "slice() %s out of bounds: offset=%d,length=%d,fileLength=%d: %s",
                    sliceDescription,
                    offset,
                    length,
                    bytesReference.length(),
                    this
                )
            );
        }
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return getOrOpenStreamInput().readByte();
        } finally {
            filePointer += 1;
        }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        getOrOpenStreamInput().readBytes(b, offset, len);
        filePointer += len;
    }

    @Override
    public short readShort() throws IOException {
        try {
            return Short.reverseBytes(getOrOpenStreamInput().readShort());
        } finally {
            filePointer += Short.BYTES;
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return Integer.reverseBytes(getOrOpenStreamInput().readInt());
        } finally {
            filePointer += Integer.BYTES;
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return Long.reverseBytes(getOrOpenStreamInput().readLong());
        } finally {
            filePointer += Long.BYTES;
        }
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public IndexInput clone() {
        return new BytesReferenceIndexInput(toString(), bytesReference, filePointer);
    }
}
