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

package org.elasticsearch.common.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteBufferReferenceTests extends AbstractBytesReferenceTestCase {

    private void initializeBytes(byte[] bytes) {
        for (int i = 0 ; i < bytes.length; ++i) {
            bytes[i] = (byte) i;
        }
    }

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        byte[] bytes = new byte[length];
        initializeBytes(bytes);
        return new ByteBufferReference(ByteBuffer.wrap(bytes));
    }
}
