/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

// NOTE: reading a block of values or ordinals is one seek on the data file, so the seeks a query makes are the blocks it decodes.
final class SeekCountingDirectory extends FilterDirectory {

    private final AtomicLong seeks;

    SeekCountingDirectory(Directory in, AtomicLong seeks) {
        super(in);
        this.seeks = seeks;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final IndexInput input = super.openInput(name, context);
        return name.endsWith(".cnd") ? new SeekCountingInput(input, seeks) : input;
    }

    private static final class SeekCountingInput extends FilterIndexInput {
        private final AtomicLong seeks;

        SeekCountingInput(IndexInput in, AtomicLong seeks) {
            super(in.toString(), in);
            this.seeks = seeks;
        }

        @Override
        public void seek(long pos) throws IOException {
            seeks.incrementAndGet();
            super.seek(pos);
        }

        @Override
        public IndexInput clone() {
            return new SeekCountingInput(in.clone(), seeks);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new SeekCountingInput(in.slice(sliceDescription, offset, length), seeks);
        }
    }
}
