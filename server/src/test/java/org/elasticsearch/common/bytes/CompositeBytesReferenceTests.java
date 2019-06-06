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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompositeBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        List<BytesReference> referenceList = newRefList(length);
        BytesReference ref = new CompositeBytesReference(referenceList.toArray(new BytesReference[0]));
        assertEquals(length, ref.length());
        return ref;
    }

    private List<BytesReference> newRefList(int length) throws IOException {
        List<BytesReference> referenceList = new ArrayList<>();
        for (int i = 0; i < length;) {
            int remaining = length-i;
            int sliceLength = randomIntBetween(1, remaining);
            ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(sliceLength, bigarrays);
            for (int j = 0; j < sliceLength; j++) {
                out.writeByte((byte) random().nextInt(1 << 8));
            }
            assertEquals(sliceLength, out.size());
            referenceList.add(out.bytes());
            i+=sliceLength;
        }
        return referenceList;
    }

    public void testCompositeBuffer() throws IOException {
        List<BytesReference> referenceList = newRefList(randomIntBetween(1, PAGE_SIZE * 2));
        BytesReference ref = new CompositeBytesReference(referenceList.toArray(new BytesReference[0]));
        BytesRefIterator iterator = ref.iterator();
        BytesRefBuilder builder = new BytesRefBuilder();

        for (BytesReference reference : referenceList) {
            BytesRefIterator innerIter = reference.iterator(); // sometimes we have a paged ref - pull an iter and walk all pages!
            BytesRef scratch;
            while ((scratch = innerIter.next()) != null) {
                BytesRef next = iterator.next();
                assertNotNull(next);
                assertEquals(next, scratch);
                builder.append(next);
            }

        }
        assertNull(iterator.next());

        int offset = 0;
        for (BytesReference reference : referenceList) {
            assertEquals(reference, ref.slice(offset, reference.length()));
            int probes = randomIntBetween(Math.min(10, reference.length()), reference.length());
            for (int i = 0; i < probes; i++) {
                int index = randomIntBetween(0, reference.length()-1);
                assertEquals(ref.get(offset + index), reference.get(index));
            }
            offset += reference.length();
        }

        BytesArray array = new BytesArray(builder.toBytesRef());
        assertEquals(array, ref);
        assertEquals(array.hashCode(), ref.hashCode());

        BytesStreamOutput output = new BytesStreamOutput();
        ref.writeTo(output);
        assertEquals(array, output.bytes());
    }

    @Override
    public void testToBytesRefSharedPage() throws IOException {
       // CompositeBytesReference doesn't share pages
    }

    @Override
    public void testSliceArrayOffset() throws IOException {
        // the assertions in this test only work on no-composite buffers
    }

    @Override
    public void testSliceToBytesRef() throws IOException {
        // CompositeBytesReference shifts offsets
    }

    public void testSliceIsNotCompositeIfMatchesSingleSubSlice() {
        CompositeBytesReference bytesRef = new CompositeBytesReference(
                new BytesArray(new byte[12]),
                new BytesArray(new byte[15]),
                new BytesArray(new byte[13]));

        // Slices that cross boundaries are composite too
        assertThat(bytesRef.slice(5, 8), Matchers.instanceOf(CompositeBytesReference.class));

        // But not slices that cover a single sub reference
        assertThat(bytesRef.slice(13, 10), Matchers.not(Matchers.instanceOf(CompositeBytesReference.class))); // strictly within sub
        assertThat(bytesRef.slice(12, 15), Matchers.not(Matchers.instanceOf(CompositeBytesReference.class))); // equal to sub
    }
}
