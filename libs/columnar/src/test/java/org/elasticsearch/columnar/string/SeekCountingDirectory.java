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

// NOTE: each read of the data file is one seek, so seeks count chunk or block reads, not decoded values.
final class SeekCountingDirectory extends FilterDirectory {

    private final Counter seeks;

    SeekCountingDirectory(Directory in, Counter seeks) {
        super(in);
        this.seeks = seeks;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final IndexInput input = super.openInput(name, context);
        return name.endsWith(".cnd") ? new SeekCountingInput(input, seeks) : input;
    }

    private static final class SeekCountingInput extends FilterIndexInput {
        private final Counter seeks;

        SeekCountingInput(IndexInput in, Counter seeks) {
            super(in.toString(), in);
            this.seeks = seeks;
        }

        @Override
        public void seek(long pos) throws IOException {
            seeks.increment();
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
