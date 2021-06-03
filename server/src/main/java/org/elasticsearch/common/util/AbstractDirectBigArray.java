/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.offheap.CleanerUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.PageCacheRecycler.LONG_PAGE_SIZE;

public abstract class AbstractDirectBigArray extends AbstractBigArray {

    private static final Logger logger = LogManager.getLogger(AbstractDirectBigArray.class);
    private final AtomicReference<ByteBuffer> directBufferRef = new AtomicReference<>();
    private volatile IntBuffer intBuffer;
    private volatile LongBuffer longBuffer;
    private volatile ByteBuffer byteBuffer;
    private final boolean clearOnResize;

    public AbstractDirectBigArray(long size, BigArrays bigArrays, boolean clearOnResize) throws IOException {
        super(LONG_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        this.clearOnResize = clearOnResize;
        this.directBufferRef.set(createDirectBuffer(numBytesPerElement(), size));
        updateRefs(directBufferRef);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public IntBuffer getIntBuffer() {
        return intBuffer;
    }

    public LongBuffer getLongBuffer() {
        return longBuffer;
    }


    @Override
    public void close() {
        if (directBufferRef.get() != null) {
            try {
                releaseBuffer(directBufferRef.getAndSet(null));
            } catch (IOException e) {
                String err = "unable to release buffer";
                logger.error(err, e);
                throw new RuntimeException(err, e);
            }
        }
    }

    @Override
    public void resize(long newSize) {
        resize(newSize, clearOnResize);
    }

    // todo synchronize?
    public void resize(long newSize, boolean clearOnResize) {
        try {
            ByteBuffer prev = this.directBufferRef.get();
            ByteBuffer newBuff = createDirectBuffer(numBytesPerElement(), newSize);
            if (clearOnResize == false) {
                cloneBytes(newBuff, prev);
            }
            assert this.directBufferRef.compareAndSet(prev, newBuff);
            updateRefs(directBufferRef);
            releaseBuffer(prev);
        } catch (Exception e) {
            String err = "failed to resize long array";
            logger.error(err, e);
            throw new RuntimeException(err, e);
        }
    }


    private ByteBuffer createDirectBuffer(int numBytesPerElement, long size) {
        return ByteBuffer.allocateDirect(numBytesPerElement * (int)size);
    }

    private void cloneBytes(ByteBuffer byteBuffer, ByteBuffer src) {
        if (src == null || src.capacity() == 0) {
            return;
        }
        src.position(0);
        byteBuffer.position(0);
        byteBuffer.put(src);
        byteBuffer.limit(byteBuffer.capacity());
        byteBuffer.position(0);
    }


    private void releaseBuffer(ByteBuffer buffer) throws IOException {
        CleanerUtil.getCleaner().freeBuffer(buffer);
    }

    private void updateRefs(AtomicReference<ByteBuffer> reference) {
        ByteBuffer byteBuffer = reference.get();
        this.intBuffer = byteBuffer.asIntBuffer();
        this.longBuffer = byteBuffer.asLongBuffer();
        this.byteBuffer = byteBuffer;
    }


}
