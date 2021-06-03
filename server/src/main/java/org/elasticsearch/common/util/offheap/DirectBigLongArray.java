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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.AbstractDirectBigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

import java.io.IOException;
import java.nio.LongBuffer;

public class DirectBigLongArray extends AbstractDirectBigArray implements LongArray {

    private static final Logger logger = LogManager.getLogger(DirectBigLongArray.class);
    private static final int NUM_BYTES_PER_ELEMENT = Long.BYTES;

    public DirectBigLongArray(long size, BigArrays bigArrays, boolean clearOnResize) throws IOException {
        super(size, bigArrays, clearOnResize);
        this.size = size;
    }


    @Override
    protected int numBytesPerElement() {
        return NUM_BYTES_PER_ELEMENT;
    }

    @Override
    public long get(long index) {
        return getLongBuffer().get((int)index);
    }

    @Override
    public long set(long index, long value) {
        LongBuffer longBuffer = getLongBuffer();
        long prev = longBuffer.get((int)index);
        getLongBuffer().put((int)index, value);
        return prev;
    }

    @Override
    public long increment(long index, long inc) {
        LongBuffer longBuffer = getLongBuffer();
        long prev = longBuffer.get((int)index);
        getLongBuffer().put((int)index, prev + inc);
        return prev;
    }

    @Override
    public void fill(long fromIndex, long toIndex, long value) {
        LongBuffer longBuffer = getLongBuffer();
        for (int i = (int)fromIndex; i<toIndex; i++) {
            longBuffer.put(i, value);
        }
    }
}
