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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;

import java.io.IOException;

public class BytesArrayTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReference(length, randomInt(length));
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        return newBytesReference(length, 0);
    }

    private BytesReference newBytesReference(int length, int offset) throws IOException {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        final BytesStreamOutput out = new BytesStreamOutput(length + offset);
        for (int i = 0; i < length + offset; i++) {
            out.writeByte((byte) random().nextInt(1 << 8));
        }
        assertEquals(length + offset, out.size());
        BytesArray ref = new BytesArray(out.bytes().toBytesRef().bytes, offset, length);
        assertEquals(length, ref.length());
        assertTrue(ref instanceof BytesArray);
        assertThat(ref.length(), Matchers.equalTo(length));
        return ref;
    }

    public void testArray() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesArray pbr = (BytesArray) newBytesReference(sizes[i]);
            byte[] array = pbr.array();
            assertNotNull(array);
            assertEquals(sizes[i], array.length - pbr.offset());
            assertSame(array, pbr.array());
        }
    }

    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesArray pbr = (BytesArray) newBytesReferenceWithOffsetOfZero(length);
        assertEquals(0, pbr.offset());
    }
}
