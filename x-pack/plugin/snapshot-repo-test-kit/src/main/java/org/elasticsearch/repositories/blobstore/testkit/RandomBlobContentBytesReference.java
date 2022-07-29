/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.AbstractBytesReference;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A {@link BytesReference} that's the same random ~8kB of data repeatedly, for use as a dummy payload for testing a blob store repo.
 */
class RandomBlobContentBytesReference extends AbstractBytesReference {

    private final int length;
    private final RandomBlobContent randomBlobContent;

    /**
     * @param randomBlobContent The (simulated) content of the blob
     * @param length            The length of this blob.
     */
    RandomBlobContentBytesReference(RandomBlobContent randomBlobContent, int length) {
        assert 0 < length;
        this.randomBlobContent = randomBlobContent;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return randomBlobContent.buffer[index % randomBlobContent.buffer.length];
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        assert false : "must not slice a RandomBlobContentBytesReference";
        throw new UnsupportedOperationException("RandomBlobContentBytesReference#slice(int, int) is unsupported");
    }

    @Override
    public long ramBytesUsed() {
        // no need for accurate accounting of the overhead since we don't really account for these things anyway
        return randomBlobContent.buffer.length;
    }

    @Override
    public BytesRef toBytesRef() {
        assert false : "must not materialize a RandomBlobContentBytesReference";
        throw new UnsupportedOperationException("RandomBlobContentBytesReference#toBytesRef() is unsupported");
    }

    @Override
    public BytesRefIterator iterator() {
        final byte[] buffer = randomBlobContent.buffer;
        final int lastBlock = (length - 1) / buffer.length;

        return new BytesRefIterator() {

            int nextBlock = 0;

            @Override
            public BytesRef next() {
                final int block = nextBlock++;
                if (block > lastBlock) {
                    return null;
                }

                randomBlobContent.ensureNotCancelled(block * buffer.length + "/" + length);
                final int end;
                if (block == lastBlock) {
                    randomBlobContent.onLastRead();
                    end = (length - 1) % buffer.length + 1;
                } else {
                    end = buffer.length;
                }

                return new BytesRef(buffer, 0, end);
            }
        };
    }
}
