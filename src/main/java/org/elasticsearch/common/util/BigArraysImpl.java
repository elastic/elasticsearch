/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.util;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;

/** Utility class to work with arrays. */
public class BigArraysImpl extends BigArrays {

    final PageCacheRecycler recycler;

    @Inject
    public BigArraysImpl(Settings settings, PageCacheRecycler recycler) {
        super(settings);
        this.recycler = recycler;
    }

    @Override
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        if (size > BYTE_PAGE_SIZE) {
            return new BigByteArray(size, recycler, clearOnResize);
        } else if (size >= BYTE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<byte[]> page = recycler.bytePage(clearOnResize);
            return new ByteArrayWrapper(page.v(), size, page, clearOnResize);
        } else {
            return new ByteArrayWrapper(new byte[(int) size], size, null, clearOnResize);
        }
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        if (size > INT_PAGE_SIZE) {
            return new BigIntArray(size, recycler, clearOnResize);
        } else if (size >= INT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<int[]> page = recycler.intPage(clearOnResize);
            return new IntArrayWrapper(page.v(), size, page, clearOnResize);
        } else {
            return new IntArrayWrapper(new int[(int) size], size, null, clearOnResize);
        }
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        if (size > LONG_PAGE_SIZE) {
            return new BigLongArray(size, recycler, clearOnResize);
        } else if (size >= LONG_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<long[]> page = recycler.longPage(clearOnResize);
            return new LongArrayWrapper(page.v(), size, page, clearOnResize);
        } else {
            return new LongArrayWrapper(new long[(int) size], size, null, clearOnResize);
        }
    }

    @Override
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        if (size > DOUBLE_PAGE_SIZE) {
            return new BigDoubleArray(size, recycler, clearOnResize);
        } else if (size >= DOUBLE_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<double[]> page = recycler.doublePage(clearOnResize);
            return new DoubleArrayWrapper(page.v(), size, page, clearOnResize);
        } else {
            return new DoubleArrayWrapper(new double[(int) size], size, null, clearOnResize);
        }
    }

    @Override
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        if (size > FLOAT_PAGE_SIZE) {
            return new BigFloatArray(size, recycler, clearOnResize);
        } else if (size >= FLOAT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<float[]> page = recycler.floatPage(clearOnResize);
            return new FloatArrayWrapper(page.v(), size, page, clearOnResize);
        } else {
            return new FloatArrayWrapper(new float[(int) size], size, null, clearOnResize);
        }
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        if (size > OBJECT_PAGE_SIZE) {
            return new BigObjectArray<>(size, recycler);
        } else if (size >= OBJECT_PAGE_SIZE / 2 && recycler != null) {
            final Recycler.V<Object[]> page = recycler.objectPage();
            return new ObjectArrayWrapper<>(page.v(), size, page);
        } else {
            return new ObjectArrayWrapper<>(new Object[(int) size], size, null);
        }
    }

}
