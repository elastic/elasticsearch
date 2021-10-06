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
import org.elasticsearch.common.util.AbstractMappedDirectBigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.file.Path;

public class MappedDirectBigIntArray extends AbstractMappedDirectBigArray implements IntArray {

    private static final Logger logger = LogManager.getLogger(MappedDirectBigIntArray.class);
    private static final int NUM_BYTES_PER_ELEMENT = Integer.BYTES;

    public MappedDirectBigIntArray(long size, BigArrays bigArrays, Path tmpPath, boolean clearOnResize) throws IOException {
        super(size, bigArrays, tmpPath, clearOnResize);
    }

    @Override
    protected int numBytesPerElement() {
        return NUM_BYTES_PER_ELEMENT;
    }

    @Override
    public int get(long index) {
        return getIntBuffer().get((int)index); // todo big index?
    }

    @Override
    public int set(long index, int value) {
        int prev = getIntBuffer().get((int)index);
        getIntBuffer().put((int)index, value);
        return prev;
    }

    @Override
    public int increment(long index, int inc) {
        int prev = get(index);
        set(index, prev + inc);
        return prev;
    }

    @Override
    public void fill(long fromIndex, long toIndex, int value) {
        IntBuffer IntBuffer = getIntBuffer();
        for (int index = (int)fromIndex; index < toIndex; index++) {
            IntBuffer.put(index, value);
        }
    }

}
