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
import org.elasticsearch.common.util.offheap.MappedTempDirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.PageCacheRecycler.LONG_PAGE_SIZE;

public abstract class AbstractMappedDirectBigArray extends AbstractBigArray {

    private static final Logger logger = LogManager.getLogger(AbstractMappedDirectBigArray.class);
    private final AtomicReference<MappedTempDirectBuffer> directBufferRef = new AtomicReference<>();
    private final Path tmpDirectoryPath;
    private volatile IntBuffer intBuffer;
    private volatile LongBuffer longBuffer;
    private volatile ByteBuffer byteBuffer;
    private final boolean clearOnResize;

    public AbstractMappedDirectBigArray(long size, BigArrays bigArrays, Path tmpPath, boolean clearOnResize) throws IOException {
        super(LONG_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        this.clearOnResize = clearOnResize;
        this.tmpDirectoryPath = tmpPath;
        this.directBufferRef.set(MappedTempDirectBuffer.createDirectBuffer(size, numBytesPerElement(), tmpPath));
        updateRefs(directBufferRef);
    }

    @Override
    public void close() {
        releaseBuffer(directBufferRef.getAndSet(null));
    }


    @Override
    public void resize(long newSize) {
        resize(newSize, clearOnResize);
    }

    public void resize(long newSize, boolean clearOnResize) {
        try {
            if (clearOnResize) {
                MappedTempDirectBuffer prev = this.directBufferRef
                    .getAndSet(MappedTempDirectBuffer.createDirectBuffer(newSize, numBytesPerElement(), tmpDirectoryPath));
                releaseBuffer(prev);
            } else {
                directBufferRef.get().resizeInPlace(newSize, numBytesPerElement());
            }
            directBufferRef.get().asByteBuffer().limit(numBytesPerElement() * (int)newSize);
            updateRefs(directBufferRef);
            this.size = newSize;
        } catch (Exception e) {
            String err = "failed to resize long array";
            logger.error(err, e);
            throw new RuntimeException(err, e);
        }
    }


    public IntBuffer getIntBuffer() {
        return intBuffer;
    }

    public LongBuffer getLongBuffer() {
        return longBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    private void updateRefs(AtomicReference<MappedTempDirectBuffer> reference) {
        MappedTempDirectBuffer byteBuffer = reference.get();
        this.intBuffer = byteBuffer.asIntBuffer();
        this.longBuffer = byteBuffer.asLongBuffer();
        this.byteBuffer = byteBuffer.asByteBuffer();
    }

    private void releaseBuffer(MappedTempDirectBuffer buffer) {
        buffer.close();
    }
}
