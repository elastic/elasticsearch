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

package org.elasticsearch.index.fielddata.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 */
public class BytesRefArrayRef {
    public static final BytesRefArrayRef EMPTY = new BytesRefArrayRef(new BytesRef[0]);

    public BytesRef[] values;
    public int start;
    public int end;

    public BytesRefArrayRef(BytesRef[] values) {
        this(values, 0, values.length);
    }

    public BytesRefArrayRef(BytesRef[] values, int length) {
        this(values, 0, length);
    }

    public BytesRefArrayRef(BytesRef[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
    }

    public void reset(int newLength) {
        assert start == 0; // NOTE: senseless if offset != 0
        end = 0;
        if (values.length < newLength) {
            values = new BytesRef[ArrayUtil.oversize(newLength, 32)];
        }
    }

    public int size() {
        return end - start;
    }
}
