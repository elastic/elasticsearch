/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Locale;

public class ES94TSDBDocValuesFormatTests extends ESTestCase {

    private static final String HOSTNAME_FIELD = "host.name";
    private static final String TIMESTAMP_FIELD = "@timestamp";

    protected final Codec codec = new Elasticsearch92Lucene103Codec() {

        final ES94TSDBDocValuesFormat docValuesFormat = new ES94TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            BinaryDVCompressionMode.NO_COMPRESS,
            true
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    protected Codec getCodec() {
        return codec;
    }

    private IndexWriterConfig getTimeSeriesIndexWriterConfig(String hostnameField, boolean multiValued, String timestampField) {
        var config = new IndexWriterConfig();
        if (hostnameField != null) {
            config.setIndexSort(
                new Sort(
                    multiValued ? new SortedSetSortField(hostnameField, false) : new SortField(hostnameField, SortField.Type.STRING, false),
                    new SortedNumericSortField(timestampField, SortField.Type.LONG, true)
                )
            );
        } else {
            config.setIndexSort(new Sort(new SortedNumericSortField(timestampField, SortField.Type.LONG, true)));
        }
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(getCodec());
        return config;
    }

    public void testNumericDocValuesRoundTrip() throws Exception {
        try (
            var dir = newDirectory();
            var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, false, TIMESTAMP_FIELD))
        ) {
            final int numDocs = 256 + random().nextInt(512);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(String.format(Locale.ROOT, "host-%03d", i % 10))));
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                d.add(new NumericDocValuesField("counter", i));
                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().get(0).reader();

                var counterDV = leaf.getNumericDocValues("counter");
                assertNotNull(counterDV);
                int count = 0;
                while (counterDV.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                    assertTrue(counterDV.longValue() >= 0 && counterDV.longValue() < numDocs);
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testSortedDocValuesRoundTrip() throws Exception {
        try (
            var dir = newDirectory();
            var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, false, TIMESTAMP_FIELD))
        ) {
            final String[] categories = new String[] { "cat-a", "cat-b", "cat-c", "cat-d" };
            final int numDocs = 256 + random().nextInt(512);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(String.format(Locale.ROOT, "host-%03d", i % 10))));
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                d.add(new SortedDocValuesField("category", new BytesRef(categories[i % categories.length])));
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                var leaf = reader.leaves().get(0).reader();

                var categoryDV = leaf.getSortedDocValues("category");
                assertNotNull(categoryDV);
                int count = 0;
                while (categoryDV.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                    String value = categoryDV.lookupOrd(categoryDV.ordValue()).utf8ToString();
                    assertTrue("unexpected category [" + value + "]", Arrays.binarySearch(categories, value) >= 0);
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testSortedSetDocValuesRoundTrip() throws Exception {
        try (
            var dir = newDirectory();
            var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, false, TIMESTAMP_FIELD))
        ) {
            final String[] tags = new String[] { "tag-a", "tag-b", "tag-c", "tag-d", "tag-e" };
            final int numDocs = 256 + random().nextInt(512);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(String.format(Locale.ROOT, "host-%03d", i % 10))));
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                int numTags = 1 + (i % 3);
                for (int j = 0; j < numTags; j++) {
                    d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[(i + j) % tags.length])));
                }
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                var leaf = reader.leaves().get(0).reader();

                var tagsDV = leaf.getSortedSetDocValues("tags");
                assertNotNull(tagsDV);
                int count = 0;
                while (tagsDV.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                    assertTrue(tagsDV.docValueCount() >= 1);
                    for (int j = 0; j < tagsDV.docValueCount(); j++) {
                        long ord = tagsDV.nextOrd();
                        String value = tagsDV.lookupOrd(ord).utf8ToString();
                        assertTrue("unexpected tag [" + value + "]", Arrays.binarySearch(tags, value) >= 0);
                    }
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testBinaryDocValuesRoundTrip() throws Exception {
        try (
            var dir = newDirectory();
            var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, false, TIMESTAMP_FIELD))
        ) {
            final int numDocs = 256 + random().nextInt(512);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(String.format(Locale.ROOT, "host-%03d", i % 10))));
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                String payload = "payload-" + i + "-" + randomAlphaOfLength(10);
                d.add(new BinaryDocValuesField("payload", new BytesRef(payload)));
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                var leaf = reader.leaves().get(0).reader();

                var payloadDV = leaf.getBinaryDocValues("payload");
                assertNotNull(payloadDV);
                int count = 0;
                while (payloadDV.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                    String value = payloadDV.binaryValue().utf8ToString();
                    assertTrue("unexpected payload", value.startsWith("payload-"));
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testSortedNumericDocValuesRoundTrip() throws Exception {
        try (
            var dir = newDirectory();
            var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, false, TIMESTAMP_FIELD))
        ) {
            final int numDocs = 256 + random().nextInt(512);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(String.format(Locale.ROOT, "host-%03d", i % 10))));
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                int numValues = 1 + (i % 5);
                for (int j = 0; j < numValues; j++) {
                    d.add(new SortedNumericDocValuesField("metrics", (i * 10L) + j));
                }
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                var leaf = reader.leaves().get(0).reader();

                var metricsDV = leaf.getSortedNumericDocValues("metrics");
                assertNotNull(metricsDV);
                int count = 0;
                while (metricsDV.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                    assertTrue(metricsDV.docValueCount() >= 1 && metricsDV.docValueCount() <= 5);
                    for (int j = 0; j < metricsDV.docValueCount(); j++) {
                        long value = metricsDV.nextValue();
                        assertTrue("unexpected metric value [" + value + "]", value >= 0);
                    }
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }
}
