/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Bytes built up in a temporary file and moved into the column once they are finished.
 *
 * <p>Several parts of a column cannot be written where they belong while they are being built: a table has
 * to know its length before it starts, and a caller feeding one is usually writing the column's values at
 * the same time. They are staged here instead, and {@link #copyInto} puts them in the column and says where
 * they landed. The temporary file goes on {@link #close}, whether or not it was ever copied.
 */
public final class StagedBytes implements Closeable {

    private final Directory directory;
    private final IndexOutput out;
    private final String name;
    private boolean copied;

    public StagedBytes(Directory directory, IOContext context, String prefix, String suffix) throws IOException {
        this.directory = directory;
        this.out = directory.createTempOutput(prefix, suffix, context);
        this.name = out.getName();
    }

    /** What the caller writes the staged bytes into. */
    public IndexOutput output() {
        return out;
    }

    /** How many bytes have been staged so far. */
    public long length() {
        return out.getFilePointer();
    }

    /**
     * Closes the staged bytes and appends them to {@code data}, answering the offset they begin at. Read
     * once, in the order they were written, and never again.
     */
    public long copyInto(IndexOutput data) throws IOException {
        out.close();
        copied = true;
        final long offset = data.getFilePointer();
        try (IndexInput in = directory.openInput(name, IOContext.READONCE)) {
            data.copyBytes(in, in.length());
        }
        return offset;
    }

    @Override
    public void close() throws IOException {
        try {
            if (copied == false) {
                IOUtils.closeWhileHandlingException(out);
            }
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(directory, name);
        }
    }
}
