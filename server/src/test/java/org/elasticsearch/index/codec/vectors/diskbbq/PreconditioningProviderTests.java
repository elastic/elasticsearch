/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

public class PreconditioningProviderTests extends LuceneTestCase {
    public void testRandomProviderConfigurations() throws IOException {
        int dim = random().nextInt(128, 1024);

        int corpusLen = random().nextInt(100, 200);
        float[][] corpus = new float[corpusLen][];
        for (int i = 0; i < corpusLen; i++) {
            corpus[i] = new float[dim];
            for (int j = 0; j < dim; j++) {
                if (j > 320) {
                    corpus[i][j] = 0f;
                } else {
                    corpus[i][j] = random().nextFloat();
                }
            }
        }

        float[] query = new float[dim];
        for (int i = 0; i < dim; i++) {
            query[i] = random().nextFloat();
        }

        int blockDim = random().nextInt(8, dim);

        PreconditioningProvider.Preconditioner preconditioner = PreconditioningProvider.createPreconditioner(dim, blockDim);

        preconditioner.applyTransform(query);

        assertEquals(blockDim, preconditioner.blockDim());
        assertEquals(dim / blockDim + 1, preconditioner.permutationMatrix().length);
        assertEquals(Math.min(blockDim, dim), preconditioner.permutationMatrix()[0].length);
        assertEquals(
            dim - (long) (dim / blockDim) * blockDim,
            preconditioner.permutationMatrix()[preconditioner.permutationMatrix().length - 1].length
        );
        assertEquals(dim / blockDim + 1, preconditioner.blocks().length);
        assertEquals(Math.min(blockDim, dim), preconditioner.blocks()[0].length);
        assertEquals(Math.min(blockDim, dim), preconditioner.blocks()[0][0].length);

        // verify can be written and read back
        PreconditioningProvider.read(new IndexInput("test") {
            byte[] data = preconditioner.toByteArray();
            int nextByte = 0;

            @Override
            public void close() throws IOException {
                // no-op
            }

            @Override
            public long getFilePointer() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void seek(long pos) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte readByte() throws IOException {
                return data[nextByte++];
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                for (int i = nextByte; i < len; i++) {
                    b[offset + (i - nextByte)] = data[i];
                }
            }
        });
    }
}
