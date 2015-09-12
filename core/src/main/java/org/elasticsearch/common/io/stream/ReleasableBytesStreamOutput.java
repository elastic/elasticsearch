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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.ReleasableBytesStream;
import org.elasticsearch.common.util.BigArrays;

/**
 * An bytes stream output that allows providing a {@link BigArrays} instance
 * expecting it to require releasing its content ({@link #bytes()}) once done.
 * <p/>
 * Please note, its is the responsibility of the caller to make sure the bytes
 * reference do not "escape" and are released only once.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements ReleasableBytesStream {

    public ReleasableBytesStreamOutput(BigArrays bigarrays) {
        super(BigArrays.PAGE_SIZE_IN_BYTES, bigarrays);
    }

    public ReleasableBytesStreamOutput(int expectedSize, BigArrays bigarrays) {
        super(expectedSize, bigarrays);
    }

    @Override
    public ReleasablePagedBytesReference bytes() {
        return new ReleasablePagedBytesReference(bigarrays, bytes, count);
    }
}
