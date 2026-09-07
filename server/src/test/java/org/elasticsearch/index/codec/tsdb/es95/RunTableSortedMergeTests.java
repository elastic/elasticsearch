/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * End-to-end validation of the run-granularity segment merge for {@code Sorted} dimension fields. Builds an
 * index-sorted, delete-free, multi-segment index through the ES95 run-table format so the optimized merge takes
 * the run-granularity path, force-merges to one segment, and asserts every merged doc carries the value its
 * series should hold. Because each host cycles through every commit, a series is split across segments and must
 * concatenate correctly; a low-cardinality dimension forces adjacent series to coalesce while a per-host
 * dimension keeps runs distinct.
 */
public class RunTableSortedMergeTests extends ESTestCase {

    private static final String HOST_FIELD = "host";
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String LOW_CARD_FIELD = "state";
    private static final String PER_HOST_FIELD = "host.ip";
    private static final String MULTI_VALUE_FIELD = "host.mac";

    public void testRunGranularityMergeProducesCorrectValues() throws IOException {
        final int numHosts = 64;
        final int numDocs = 6000;
        final int commitEvery = 400;

        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    final int host = i % numHosts;
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(HOST_FIELD, new BytesRef(hostName(host))));
                    doc.add(new SortedDocValuesField(LOW_CARD_FIELD, new BytesRef(stateOf(host))));
                    doc.add(new SortedDocValuesField(PER_HOST_FIELD, new BytesRef(ipOf(host))));
                    for (final String mac : macsOf(host)) {
                        doc.add(new SortedSetDocValuesField(MULTI_VALUE_FIELD, new BytesRef(mac)));
                    }
                    doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, 1_000L + i));
                    writer.addDocument(doc);
                    if (i % commitEvery == commitEvery - 1) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals("force-merge should leave one segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals(numDocs, leaf.maxDoc());

                final SortedDocValues host = leaf.getSortedDocValues(HOST_FIELD);
                final SortedDocValues state = leaf.getSortedDocValues(LOW_CARD_FIELD);
                final SortedDocValues ip = leaf.getSortedDocValues(PER_HOST_FIELD);
                final SortedSetDocValues mac = leaf.getSortedSetDocValues(MULTI_VALUE_FIELD);

                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    assertTrue("host present at doc " + doc, host.advanceExact(doc));
                    final String hostValue = host.lookupOrd(host.ordValue()).utf8ToString();

                    assertTrue("state present at doc " + doc, state.advanceExact(doc));
                    assertEquals("doc " + doc, expectedState(hostValue), state.lookupOrd(state.ordValue()).utf8ToString());

                    assertTrue("ip present at doc " + doc, ip.advanceExact(doc));
                    assertEquals("doc " + doc, expectedIp(hostValue), ip.lookupOrd(ip.ordValue()).utf8ToString());

                    assertTrue("mac present at doc " + doc, mac.advanceExact(doc));
                    final String[] expectedMacs = expectedMacs(hostValue);
                    assertEquals("doc " + doc + " mac count", expectedMacs.length, mac.docValueCount());
                    for (final String expectedMac : expectedMacs) {
                        assertEquals("doc " + doc, expectedMac, mac.lookupOrd(mac.nextOrd()).utf8ToString());
                    }
                }
            }
        }
    }

    public void testMergeWithDeletionsFallsBackAndStaysCorrect() throws IOException {
        // Deletions make the source segments carry live-docs, so `compatibleWithOptimizedMerge` bails and the merge
        // takes Lucene's per-doc path (not run-granularity). The merged dimension values must still be correct.
        final int numHosts = 32;
        final int numDocs = 3000;
        final int commitEvery = 300;
        final int deleteEvery = 7;

        try (Directory directory = newDirectory()) {
            int deleted = 0;
            try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    final int host = i % numHosts;
                    final Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    doc.add(new SortedDocValuesField(HOST_FIELD, new BytesRef(hostName(host))));
                    doc.add(new SortedDocValuesField(LOW_CARD_FIELD, new BytesRef(stateOf(host))));
                    doc.add(new SortedDocValuesField(PER_HOST_FIELD, new BytesRef(ipOf(host))));
                    for (final String mac : macsOf(host)) {
                        doc.add(new SortedSetDocValuesField(MULTI_VALUE_FIELD, new BytesRef(mac)));
                    }
                    doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, 1_000L + i));
                    writer.addDocument(doc);
                    if (i % commitEvery == commitEvery - 1) {
                        writer.commit();
                    }
                }
                for (int i = 0; i < numDocs; i += deleteEvery) {
                    writer.deleteDocuments(new Term("id", Integer.toString(i)));
                    deleted++;
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals(numDocs - deleted, leaf.maxDoc());

                final SortedDocValues host = leaf.getSortedDocValues(HOST_FIELD);
                final SortedDocValues state = leaf.getSortedDocValues(LOW_CARD_FIELD);
                final SortedDocValues ip = leaf.getSortedDocValues(PER_HOST_FIELD);
                final SortedSetDocValues mac = leaf.getSortedSetDocValues(MULTI_VALUE_FIELD);

                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    assertTrue("host present at doc " + doc, host.advanceExact(doc));
                    final String hostValue = host.lookupOrd(host.ordValue()).utf8ToString();

                    assertTrue("state present at doc " + doc, state.advanceExact(doc));
                    assertEquals("doc " + doc, expectedState(hostValue), state.lookupOrd(state.ordValue()).utf8ToString());

                    assertTrue("ip present at doc " + doc, ip.advanceExact(doc));
                    assertEquals("doc " + doc, expectedIp(hostValue), ip.lookupOrd(ip.ordValue()).utf8ToString());

                    assertTrue("mac present at doc " + doc, mac.advanceExact(doc));
                    final String[] expectedMacs = expectedMacs(hostValue);
                    assertEquals("doc " + doc + " mac count", expectedMacs.length, mac.docValueCount());
                    for (final String expectedMac : expectedMacs) {
                        assertEquals("doc " + doc, expectedMac, mac.lookupOrd(mac.nextOrd()).utf8ToString());
                    }
                }
            }
        }
    }

    private static String hostName(int host) {
        return "host-" + host;
    }

    private static String stateOf(int host) {
        return "state-" + (host % 5);
    }

    private static String ipOf(int host) {
        return "10.0.0." + host;
    }

    private static String[] macsOf(int host) {
        // Two distinct MACs per host, constant per host, ascending by term so they land in ordinal order.
        return new String[] { "mac-" + (host % 4), "mac-" + ((host % 4) + 4) };
    }

    private static String expectedState(String hostName) {
        return stateOf(Integer.parseInt(hostName.substring("host-".length())));
    }

    private static String expectedIp(String hostName) {
        return ipOf(Integer.parseInt(hostName.substring("host-".length())));
    }

    private static String[] expectedMacs(String hostName) {
        return macsOf(Integer.parseInt(hostName.substring("host-".length())));
    }

    private static IndexWriterConfig indexWriterConfig() {
        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setIndexSort(
            new Sort(
                new SortField(HOST_FIELD, SortField.Type.STRING, false),
                new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
            )
        );
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        final FieldContextResolver resolver = (fieldName, blockSize) -> new FieldContext(
            blockSize,
            fieldName,
            null,
            null,
            fieldName.equals(LOW_CARD_FIELD) || fieldName.equals(PER_HOST_FIELD) || fieldName.equals(MULTI_VALUE_FIELD)
        );
        final DocValuesFormat docValuesFormat = ES95TSDBDocValuesFormatFactory.create(true, true, false, resolver, true);
        config.setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        });
        return config;
    }
}
