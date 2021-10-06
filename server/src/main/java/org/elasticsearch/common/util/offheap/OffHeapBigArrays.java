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

package org.elasticsearch.common.util.offheap;

import org.elasticsearch.common.util.AbstractDirectBigArray;
import org.elasticsearch.common.util.AbstractMappedDirectBigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.nio.file.Path;

public class OffHeapBigArrays extends BigArrays {

    private final Path tmpPath;
    private final OffHeapDecider offHeapDecider;

    public OffHeapBigArrays(PageCacheRecycler recycler, CircuitBreakerService breakerService, String breakerName, boolean checkBreaker, Path tmpPath, OffHeapDecider offHeapDecider) {
        super(recycler, breakerService, breakerName, checkBreaker);
        this.tmpPath = tmpPath;
        this.offHeapDecider = offHeapDecider;
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        try {
            return newOffHeapLongArray(size, clearOnResize);
        } catch (IOException e) {
            throw new RuntimeException("create off-heap long array failed. size: " + size, e);
        }
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        try {
            return newOffHeapIntArray(size, clearOnResize);
        } catch (IOException e) {
            throw new RuntimeException("create off-heap long array failed. size: " + size, e);
        }
    }

    @Override
    public IntArray resize(IntArray array, long size) {
        if (array instanceof AbstractMappedDirectBigArray) {
            ((AbstractMappedDirectBigArray)array).resize(size, false);
            return array;
        } else if (array instanceof AbstractDirectBigArray) {
            ((AbstractDirectBigArray)array).resize(size, false);
            return array;
        } else {
            return super.resize(array, size);
        }
    }



    @Override
    public LongArray resize(LongArray array, long size) {
        if (array instanceof AbstractMappedDirectBigArray) {
            ((AbstractMappedDirectBigArray)array).resize(size, false);
            return array;
        } else if (array instanceof AbstractDirectBigArray) {
            ((AbstractDirectBigArray)array).resize(size, false);
            return array;
        } else {
            return super.resize(array, size);
        }
    }


    private LongArray newOffHeapLongArray(long size, boolean clearOnResize) throws IOException {
        LongArray array;
        long sizeInBytes = size * Long.BYTES;
        OffHeapDecider.Decision decision =  offHeapDecider.decide(sizeInBytes);
        switch (decision) {
            case None: {
                array = super.newLongArray(size, clearOnResize);
                break;
            }
            case Direct: {
                array = new DirectBigLongArray(size, this, clearOnResize);
                break;
            }
            case Mapped: {
                array = new MappedDirectBigLongArray(size, this, tmpPath, clearOnResize);
                break;
            }
            default: {
                throw new UnsupportedOperationException("unsupported decision: " + decision);
            }
        }
        return array;
    }


    private IntArray newOffHeapIntArray(long size, boolean clearOnResize) throws IOException {
        IntArray array;
        long sizeInBytes = size * Long.BYTES;
        OffHeapDecider.Decision decision =  offHeapDecider.decide(sizeInBytes);
        switch (decision) {
            case None: {
                array = super.newIntArray(size, clearOnResize);
                break;
            }
            case Direct: {
                array = new DirectBigIntArray(size, this, clearOnResize);
                break;
            }
            case Mapped: {
                array = new MappedDirectBigIntArray(size, this, tmpPath, clearOnResize);
                break;
            }
            default: {
                throw new UnsupportedOperationException("unsupported decision: " + decision);
            }
        }
        return array;
    }
}
