package org.elasticsearch.index.fielddata.ordinals;

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 * An efficient store for positive integer slices. This pool uses multiple
 * sliced arrays to hold integers in int array pages rather than an object based
 * datastructures.
 */
final class PositiveIntPool {
    // TODO it might be useful to store the size of the slices in a sep
    // datastructure rather than useing a negative value to donate this.
    private final int blockShift;
    private final int blockMask;
    private final int blockSize;
    /**
     * array of buffers currently used in the pool. Buffers are allocated if
     * needed don't modify this outside of this class
     */
    private int[][] buffers = new int[10][];

    /**
     * index into the buffers array pointing to the current buffer used as the
     * head
     */
    private int bufferUpto = -1;
    /** Pointer to the current position in head buffer */
    private int intUpto;
    /** Current head buffer */
    private int[] buffer;
    /** Current head offset */
    private int intOffset;


    /**
     * Creates a new {@link PositiveIntPool} with the given blockShift.
     * 
     * @param blockShift
     *            the n-the power of two indicating the size of each block in
     *            the paged datastructure. BlockSize = 1 << blockShift
     */
    public PositiveIntPool(int blockShift) {
        this.blockShift = blockShift;
        this.blockSize = 1 << blockShift;
        this.blockMask = blockSize - 1;
        this.intUpto = blockSize;
        this.intOffset = -blockSize;
    }

    /**
     * Adds all integers in the given slices and returns the positive offset
     * into the datastructure to retrive this slice.
     */
    public int put(IntArrayRef slice) {
        int length = slice.end - slice.start;
        if ( length > blockSize) {
            throw new ElasticSearchIllegalArgumentException("Can not store slices greater or equal to: " + blockSize);
        }
        if ((intUpto + length) > blockSize) {
            nextBuffer();
        }
        final int relativeOffset = intUpto;
        System.arraycopy(slice.values, slice.start, buffer, relativeOffset, length);
        intUpto += length;
        buffer[intUpto - 1] *= -1; // mark as end
        return relativeOffset + intOffset;
    }

    /**
     * Returns the first value of the slice stored at the given offset.
     * <p>
     * Note: the slice length must be greater than one otherwise the returned
     * value is the negative complement of the actual value
     * </p>
     */
    public int getFirstFromOffset(int offset) {
        final int blockOffset = offset >> blockShift;
        final int relativeOffset = offset & blockMask;
        final int[] currentBuffer = buffers[blockOffset];
        assert currentBuffer[relativeOffset] >= 0;
        return currentBuffer[relativeOffset];
    }

    /**
     * Retrieves a previously stored slice from the pool.
     * 
     * @param slice the sclice to fill
     * @param offset the offset where the slice is stored
     */
    public void fill(IntArrayRef slice, int offset) {
        final int blockOffset = offset >> blockShift;
        final int relativeOffset = offset & blockMask;
        final int[] currentBuffer = buffers[blockOffset];
        slice.start = 0;
        slice.end = 0;
        for (int i = relativeOffset; i < currentBuffer.length; i++) {
            slice.end++;
            if (currentBuffer[i] < 0) {
                break;
            }
            
        }
        if (slice.end != 0) {
            slice.values = ArrayUtil.grow(slice.values, slice.end);
            System.arraycopy(currentBuffer, relativeOffset, slice.values, 0, slice.end);
            slice.values[slice.end-1] *= -1;
        }
    }

    public long getMemorySizeInBytes() {
        return ((bufferUpto + 1) * blockSize * RamUsage.NUM_BYTES_INT) + ((bufferUpto + 1) * RamUsage.NUM_BYTES_ARRAY_HEADER);
    }

    private void nextBuffer() {
        if (1 + bufferUpto == buffers.length) {
            int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
            buffers = newBuffers;
        }
        buffer = buffers[1 + bufferUpto] = new int[blockSize];
        bufferUpto++;
        intUpto = 0;
        intOffset += blockSize;
    }
    
}
