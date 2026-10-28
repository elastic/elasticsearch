/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class SortedFieldObserverTests extends ESTestCase {

    public void testNoopObserverDoesNothing() throws IOException {
        final SortedFieldObserver observer = SortedFieldObserver.NOOP;
        observer.onTerm(new BytesRef("test"), 0);
        observer.prepareForDocs();
        observer.onDoc(0, 0);

        final ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        final ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        try (
            IndexOutput data = new ByteBuffersIndexOutput(dataOut, "data", "data");
            IndexOutput meta = new ByteBuffersIndexOutput(metaOut, "meta", "meta")
        ) {
            observer.flush(data, meta);
        }
        assertEquals(0, dataOut.size());
        assertEquals(0, metaOut.size());
    }

    public void testPrefixPartitionsWriterRoundTrip() throws IOException {
        final SortedFieldObserver observer = new PrefixedPartitionsWriter();

        final byte[] term1Bytes = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        final byte[] term2Bytes = new byte[] { 0x02, 0x00, 0x00, 0x00 };

        observer.onTerm(new BytesRef(term1Bytes), 0);
        observer.onTerm(new BytesRef(term2Bytes), 1);
        observer.prepareForDocs();
        observer.onDoc(0, 0);
        observer.onDoc(10, 1);

        try (Directory dir = newDirectory()) {
            try (
                IndexOutput data = dir.createOutput("data", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)
            ) {
                observer.flush(data, meta);
            }

            try (IndexInput dataInput = dir.openInput("data", IOContext.DEFAULT)) {
                PartitionedDocValues.PrefixPartitions partitions = PrefixedPartitionsReader.prefixPartitions(dataInput, null);

                assertEquals(2, partitions.numPartitions());
                assertEquals(0, partitions.startDocs()[0]);
                assertEquals(10, partitions.startDocs()[1]);
            }
        }
    }

    public void testSinglePartitionRoundTrip() throws IOException {
        final PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();

        final byte[] termBytes = new byte[] { 0x05, 0x00, 0x00, 0x00 };
        writer.onTerm(new BytesRef(termBytes), 0);
        writer.prepareForDocs();
        writer.onDoc(0, 0);

        try (Directory dir = newDirectory()) {
            try (
                IndexOutput data = dir.createOutput("data", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)
            ) {
                writer.flush(data, meta);
            }

            try (IndexInput dataInput = dir.openInput("data", IOContext.DEFAULT)) {
                PartitionedDocValues.PrefixPartitions partitions = PrefixedPartitionsReader.prefixPartitions(dataInput, null);

                assertEquals(1, partitions.numPartitions());
                assertEquals(0, partitions.startDocs()[0]);
            }
        }
    }

    public void testReaderReusesExistingPartitions() throws IOException {
        final PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();

        final byte[] term1Bytes = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        final byte[] term2Bytes = new byte[] { 0x02, 0x00, 0x00, 0x00 };
        final byte[] term3Bytes = new byte[] { 0x03, 0x00, 0x00, 0x00 };

        writer.onTerm(new BytesRef(term1Bytes), 0);
        writer.onTerm(new BytesRef(term2Bytes), 1);
        writer.onTerm(new BytesRef(term3Bytes), 2);
        writer.prepareForDocs();
        writer.onDoc(0, 0);
        writer.onDoc(100, 1);
        writer.onDoc(200, 2);

        try (Directory dir = newDirectory()) {
            try (
                IndexOutput data = dir.createOutput("data", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)
            ) {
                writer.flush(data, meta);
            }

            try (IndexInput dataInput = dir.openInput("data", IOContext.DEFAULT)) {
                final PartitionedDocValues.PrefixPartitions first = PrefixedPartitionsReader.prefixPartitions(dataInput, null);
                assertEquals(3, first.numPartitions());
                assertEquals(0, first.startDocs()[0]);
                assertEquals(100, first.startDocs()[1]);
                assertEquals(200, first.startDocs()[2]);

                dataInput.seek(0);
                final PartitionedDocValues.PrefixPartitions reused = PrefixedPartitionsReader.prefixPartitions(dataInput, first);
                assertEquals(3, reused.numPartitions());
                assertSame(first.prefixes(), reused.prefixes());
                assertSame(first.startDocs(), reused.startDocs());
            }
        }
    }

    public void testReaderAllocatesWhenReusedTooSmall() throws IOException {
        final PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();

        final byte[] term1Bytes = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        final byte[] term2Bytes = new byte[] { 0x02, 0x00, 0x00, 0x00 };
        final byte[] term3Bytes = new byte[] { 0x03, 0x00, 0x00, 0x00 };

        writer.onTerm(new BytesRef(term1Bytes), 0);
        writer.onTerm(new BytesRef(term2Bytes), 1);
        writer.onTerm(new BytesRef(term3Bytes), 2);
        writer.prepareForDocs();
        writer.onDoc(0, 0);
        writer.onDoc(50, 1);
        writer.onDoc(99, 2);

        try (Directory dir = newDirectory()) {
            try (
                IndexOutput data = dir.createOutput("data", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)
            ) {
                writer.flush(data, meta);
            }

            try (IndexInput dataInput = dir.openInput("data", IOContext.DEFAULT)) {
                PartitionedDocValues.PrefixPartitions tooSmall = new PartitionedDocValues.PrefixPartitions(1, new int[1], new int[1]);
                PartitionedDocValues.PrefixPartitions result = PrefixedPartitionsReader.prefixPartitions(dataInput, tooSmall);
                assertEquals(3, result.numPartitions());
                assertNotSame(tooSmall.prefixes(), result.prefixes());
                assertNotSame(tooSmall.startDocs(), result.startDocs());
                assertEquals(0, result.startDocs()[0]);
                assertEquals(50, result.startDocs()[1]);
                assertEquals(99, result.startDocs()[2]);
            }
        }
    }

    public void testMultipleTermsSamePrefix() throws IOException {
        final PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();

        // These two terms share the same 18-bit prefix (first 18 bits of 0x01000000 and 0x01000100 are identical)
        final byte[] term1Bytes = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        final byte[] term2Bytes = new byte[] { 0x01, 0x00, 0x01, 0x00 };
        final byte[] term3Bytes = new byte[] { 0x02, 0x00, 0x00, 0x00 };

        writer.onTerm(new BytesRef(term1Bytes), 0);
        writer.onTerm(new BytesRef(term2Bytes), 1);
        writer.onTerm(new BytesRef(term3Bytes), 2);
        writer.prepareForDocs();
        writer.onDoc(0, 0);
        writer.onDoc(5, 1);
        writer.onDoc(10, 2);

        try (Directory dir = newDirectory()) {
            try (
                IndexOutput data = dir.createOutput("data", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT)
            ) {
                writer.flush(data, meta);
            }

            try (IndexInput dataInput = dir.openInput("data", IOContext.DEFAULT)) {
                PartitionedDocValues.PrefixPartitions partitions = PrefixedPartitionsReader.prefixPartitions(dataInput, null);

                assertEquals(2, partitions.numPartitions());
                assertEquals(0, partitions.startDocs()[0]);
                assertEquals(10, partitions.startDocs()[1]);
            }
        }
    }

    public void testFactoryNoopWhenDisabled() {
        final SortedFieldObserverFactory factory = SortedFieldObserverFactory.NOOP;
        final FieldInfo fieldInfo = new FieldInfo(
            "test_field",
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            false,
            false
        );
        final SortedFieldObserver observer = factory.create(fieldInfo);
        assertSame(SortedFieldObserver.NOOP, observer);
    }
}
