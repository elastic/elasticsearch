/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Wrapper around {@link DocVector} to make a valid {@link Block}.
 */
public class DocBlock extends AbstractVectorBlock implements Block {

    private final DocVector vector;

    DocBlock(DocVector vector) {
        this.vector = vector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocVector asVector() {
        return vector;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC;
    }

    @Override
    public Block filter(int... positions) {
        return new DocBlock(asVector().filter(positions));
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocBlock");
    }

    @Override
    public DocBlock expand() {
        incRef();
        return this;
    }

    @Override
    public int hashCode() {
        return vector.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocBlock == false) {
            return false;
        }
        return this == obj || vector.equals(((DocBlock) obj).vector);
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public void closeInternal() {
        assert (vector.isReleased() == false) : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    /**
     * A builder the for {@link DocBlock}.
     */
    public static Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new Builder(blockFactory, estimatedSize);
    }

    public static class Builder implements Block.Builder {
        private final IntVector.Builder shards;
        private final IntVector.Builder segments;
        private final IntVector.Builder docs;

        private Builder(BlockFactory blockFactory, int estimatedSize) {
            IntVector.Builder shards = null;
            IntVector.Builder segments = null;
            IntVector.Builder docs = null;
            try {
                shards = blockFactory.newIntVectorBuilder(estimatedSize);
                segments = blockFactory.newIntVectorBuilder(estimatedSize);
                docs = blockFactory.newIntVectorBuilder(estimatedSize);
            } finally {
                if (docs == null) {
                    Releasables.closeExpectNoException(shards, segments, docs);
                }
            }
            this.shards = shards;
            this.segments = segments;
            this.docs = docs;
        }

        public Builder appendShard(int shard) {
            shards.appendInt(shard);
            return this;
        }

        public Builder appendSegment(int segment) {
            segments.appendInt(segment);
            return this;
        }

        public Builder appendDoc(int doc) {
            docs.appendInt(doc);
            return this;
        }

        @Override
        public Builder appendNull() {
            throw new UnsupportedOperationException("doc blocks can't contain null");
        }

        @Override
        public Builder beginPositionEntry() {
            throw new UnsupportedOperationException("doc blocks only contain one value per position");
        }

        @Override
        public Builder endPositionEntry() {
            throw new UnsupportedOperationException("doc blocks only contain one value per position");
        }

        @Override
        public Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            DocVector docVector = ((DocBlock) block).asVector();
            for (int i = beginInclusive; i < endExclusive; i++) {
                shards.appendInt(docVector.shards().getInt(i));
                segments.appendInt(docVector.segments().getInt(i));
                docs.appendInt(docVector.docs().getInt(i));
            }
            return this;
        }

        @Override
        public Block.Builder mvOrdering(MvOrdering mvOrdering) {
            /*
             * This is called when copying but otherwise doesn't do
             * anything because there aren't multivalue fields in a
             * block containing doc references. Every position can
             * only reference one doc.
             */
            return this;
        }

        @Override
        public long estimatedBytes() {
            return DocVector.BASE_RAM_BYTES_USED + shards.estimatedBytes() + segments.estimatedBytes() + docs.estimatedBytes();
        }

        @Override
        public DocBlock build() {
            // Pass null for singleSegmentNonDecreasing so we calculate it when we first need it.
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            DocVector result = null;
            try {
                shards = this.shards.build();
                segments = this.segments.build();
                docs = this.docs.build();
                result = new DocVector(shards, segments, docs, null);
                return result.asBlock();
            } finally {
                if (result == null) {
                    Releasables.closeExpectNoException(shards, segments, docs);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(shards, segments, docs);
        }
    }

    @Override
    public void allowPassingToDifferentDriver() {
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public int getPositionCount() {
        return vector.getPositionCount();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }
}
