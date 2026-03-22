/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PrefixPartitionTests extends ESTestCase {

    private Codec getCodec(boolean writePrefixPartitions) {
        return TestUtil.alwaysDocValuesFormat(
            new ES819Version3TSDBDocValuesFormat(
                random().nextInt(4, 16),
                random().nextInt(1, 32),
                random().nextBoolean(),
                ES819TSDBDocValuesFormatTests.randomBinaryCompressionMode(),
                random().nextBoolean(),
                ES819TSDBDocValuesFormatTests.randomNumericBlockSize(),
                writePrefixPartitions
            )
        );
    }

    private PartitionedDocValues.PrefixPartitions runTest(List<BytesRef> values) throws Exception {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec(false))
            .setIndexSort(new Sort(new SortField("first", SortField.Type.STRING)))
            .setMaxBufferedDocs(1000_000)
            .setRAMBufferSizeMB(100);
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, config)) {
            for (BytesRef value : values) {
                final Document doc = new Document();
                SortedDocValuesField dv = new SortedDocValuesField("first", value);
                doc.add(dv);
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves(), hasSize(1));
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedDocValues dv = leaf.getSortedDocValues("first");
                assertThat(dv, instanceOf(PartitionedDocValues.class));
                PartitionedDocValues partitionedDV = (PartitionedDocValues) dv;
                assertThat(partitionedDV.prefixPartitionBits(), equalTo(0));
            }
        }
        config = new IndexWriterConfig().setCodec(getCodec(true))
            .setIndexSort(new Sort(new SortField("first", SortField.Type.STRING)))
            .setMaxBufferedDocs(1000_000)
            .setRAMBufferSizeMB(100);
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, config)) {
            for (BytesRef value : values) {
                final Document doc = new Document();
                SortedDocValuesField dv = new SortedDocValuesField("first", value);
                doc.add(dv);
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves(), hasSize(1));
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedDocValues dv = leaf.getSortedDocValues("first");
                assertThat(dv, instanceOf(PartitionedDocValues.class));
                PartitionedDocValues partitionedDV = (PartitionedDocValues) dv;
                assertThat(partitionedDV.prefixPartitionBits(), equalTo(18));
                var partitions = partitionedDV.prefixPartitions(null);
                assertNotNull(partitions);
                return partitions;
            }
        }
    }

    public void testSingleTerm() throws Exception {
        BytesRef term = new BytesRef(new byte[] { 8, 5, 3, 4 });
        int numDocs = between(1, 100);
        List<BytesRef> terms = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            terms.add(term);
        }
        PartitionedDocValues.PrefixPartitions partitions = runTest(terms);
        assertThat(partitions.numPartitions(), equalTo(1));
        assertThat(partitions.prefixes(), equalTo(new int[] { 0x2014 }));
        assertThat(partitions.startDocs(), equalTo(new int[] { 0 }));
    }

    public void testSomeTerms() throws Exception {
        Map<BytesRef, Integer> docsPerTerm = Map.of(
            new BytesRef(new byte[] { 1, 2, 1, 3 }),
            3,
            new BytesRef(new byte[] { 1, 2, 2, 5 }),
            4,
            new BytesRef(new byte[] { 1, 2, 3, 7 }),
            1,
            new BytesRef(new byte[] { 3, 4, 1, 9 }),
            5,
            new BytesRef(new byte[] { 3, 4, 2, 11 }),
            6,
            new BytesRef(new byte[] { (byte) 255, 0, 0, 13 }),
            9
        );
        List<BytesRef> terms = new ArrayList<>();
        for (Map.Entry<BytesRef, Integer> e : docsPerTerm.entrySet()) {
            for (int i = 0; i < e.getValue(); i++) {
                terms.add(e.getKey());
            }
        }
        Randomness.shuffle(terms);
        PartitionedDocValues.PrefixPartitions partitions = runTest(terms);
        assertThat(partitions.numPartitions(), equalTo(3));
        assertThat(partitions.prefixes(), equalTo(new int[] { 0x408, 0xC10, 0x3FC00 }));
        assertThat(partitions.startDocs(), equalTo(new int[] { 0, 8, 19 }));
    }

    public void testRandom() throws Exception {
        Set<BytesRef> dict = new TreeSet<>();
        int numMetrics = between(1, 16);
        byte[] metrics = new byte[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            metrics[i] = randomByte();
        }
        int numTerms = randomIntBetween(1, 5_000);
        while (dict.size() < numTerms) {
            byte[] bytes = randomByteArrayOfLength(4);
            bytes[0] = metrics[random().nextInt(metrics.length)];
            dict.add(new BytesRef(bytes));
        }
        List<BytesRef> terms = new ArrayList<>(dict.size() * 10);
        Map<Integer, Integer> prefixToStartDocs = new TreeMap<>();
        int docId = 0;
        for (BytesRef term : dict) {
            int prefix = (int) PrefixedPartitionsWriter.BE_INT.get(term.bytes, term.offset) >>> (Integer.SIZE
                - PrefixedPartitionsWriter.PARTITION_PREFIX_BITS);
            prefixToStartDocs.putIfAbsent(prefix, docId);
            int numDocs = between(1, 5);
            for (int d = 0; d < numDocs; d++) {
                terms.add(term);
            }
            docId += numDocs;
        }
        Randomness.shuffle(terms);
        PartitionedDocValues.PrefixPartitions partitions = runTest(terms);
        assertThat(partitions.numPartitions(), equalTo(prefixToStartDocs.size()));
        int idx = 0;
        for (Map.Entry<Integer, Integer> e : prefixToStartDocs.entrySet()) {
            int prefix = e.getKey();
            int startDoc = e.getValue();
            assertThat("idx=" + idx, partitions.prefixes()[idx], equalTo(prefix));
            assertThat("idx=" + idx, partitions.startDocs()[idx], equalTo(startDoc));
            idx++;
        }
    }
}
