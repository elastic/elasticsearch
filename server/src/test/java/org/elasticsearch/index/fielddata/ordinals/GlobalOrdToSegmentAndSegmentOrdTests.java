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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Reader;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.ReaderProvider;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Writer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequenceTests.neverWrites;
import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequenceTests.noFilesLeftBehindDir;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class GlobalOrdToSegmentAndSegmentOrdTests extends ESTestCase {
    private static final int MAX = 100000;

    public void testEmpty() throws IOException {
        try (OrdinalSequence.GroupWriter groupWriter = neverWrites()) {
            Writer writer = new Writer(groupWriter, 0, 0);
            assertIdentity(groupWriter, writer);
        }
    }

    public void testAllInFirstSegment() throws IOException {
        try (OrdinalSequence.GroupWriter groupWriter = neverWrites()) {
            Writer writer = new Writer(groupWriter, 0, 0);
            int count = randomInt(MAX);
            for (int i = 0; i < count; i++) {
                writer.write(i, 0, i);
            }
            assertIdentity(groupWriter, writer);
        }
    }

    private void assertIdentity(OrdinalSequence.GroupWriter groupWriter, Writer writer) throws IOException {
        try (OrdinalSequence.GroupReader groupReader = groupWriter.finish()) {
            ReaderProvider provider = writer.readerProvider();
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            Reader reader = provider.get(groupReader.input());
            long l = randomLong();
            assertThat(reader.containingSegment(l), equalTo(0));
            assertThat(reader.containingSegmentOrd(l), equalTo(l));
        }
    }

    public void testAllInOneSegment() throws IOException {
        int count = randomInt(1000);
        try (
            Directory directory = noFilesLeftBehindDir();
            OrdinalSequence.GroupWriter groupWriter = OrdinalSequence.GroupWriter.tmpFile("test", "test", directory);
        ) {
            Writer writer = new Writer(groupWriter, count, count + (randomBoolean() ? 0 : between(1, MAX)));
            int[] expectedSegment = new int[count];
            long[] expectedSegmentOrd = new long[count];
            for (int i = 0; i < count; i++) {
                writer.write(i, i, 0);
                expectedSegment[i] = i;
                expectedSegmentOrd[i] = 0;
            }
            assertExpected(groupWriter, expectedSegment, expectedSegmentOrd, writer);
        }
    }

    public void testAllInFewSegments() throws IOException {
        int count = between(1, MAX);
        for (int segmentCount : new int[] { 2, 10, 50, 100, 1000 }) {
            long[] segmentOrds = new long[segmentCount];

            int[] expectedSegment = new int[count];
            long[] expectedSegmentOrd = new long[count];
            for (int i = 0; i < count; i++) {
                int segment = randomInt(segmentCount - 1);
                expectedSegment[i] = segment;
                expectedSegmentOrd[i] = segmentOrds[segment];
                segmentOrds[segment]++;
            }

            try (
                Directory directory = noFilesLeftBehindDir();
                OrdinalSequence.GroupWriter groupWriter = OrdinalSequence.GroupWriter.tmpFile("test", "test", directory);
            ) {
                Writer writer = new Writer(groupWriter, count, count + (randomBoolean() ? 0 : between(1, count)));
                for (int i = 0; i < count; i++) {
                    writer.write(i, expectedSegment[i], expectedSegmentOrd[i]);
                }
                assertExpected(groupWriter, expectedSegment, expectedSegmentOrd, writer);
            }
        }
    }

    private void assertExpected(OrdinalSequence.GroupWriter groupWriter, int[] expectedSegment, long[] expectedSegmentOrd, Writer writer)
        throws IOException {
        try (OrdinalSequence.GroupReader groupReader = groupWriter.finish()) {
            ReaderProvider provider = writer.readerProvider();
            assertThat(expectedSegmentOrd.length, equalTo(expectedSegment.length));
            assertThat(provider.ramBytesUsed(), greaterThan(100L));
            assertThat(provider.ramBytesUsed(), lessThan(1000L));
            Reader reader = provider.get(groupReader.input());
            for (int i = 0; i < expectedSegment.length; i++) {
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
            // And again with random access
            for (int n = 0; n < 1000; n++) {
                int i = between(0, expectedSegment.length - 1);
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
        }
    }
}
