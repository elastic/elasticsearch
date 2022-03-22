/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;

/** Implements reverse read from a RandomAccessInput. */
final class ReverseRandomAccessReader extends FST.BytesReader {
    private final RandomAccessInput in;
    private long pos;

    ReverseRandomAccessReader(RandomAccessInput in) {
        this.in = in;
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte(pos--);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        int i = offset, end = offset + len;
        while (i < end) {
            b[i++] = in.readByte(pos--);
        }
    }

    @Override
    public void skipBytes(long count) {
        pos -= count;
    }

    @Override
    public long getPosition() {
        return pos;
    }

    @Override
    public void setPosition(long pos) {
        this.pos = pos;
    }

    @Override
    public boolean reversed() {
        return true;
    }
}
