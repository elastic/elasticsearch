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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

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
                while (counterDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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
                while (categoryDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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
                while (tagsDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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
                while (payloadDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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
                while (metricsDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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

    public void testPerFieldPipelineNumericRoundTrip() throws Exception {
        final Map<String, PipelineConfig> fieldPipelines = Map.of(
            "double_gorilla",
            PipelineConfig.forDoubles(randomBlockSize()).gorilla(),
            "double_alp",
            PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().offset().gcd().bitPack(),
            "double_alp_rd",
            PipelineConfig.forDoubles(randomBlockSize()).alpRdDoubleStage().offset().gcd().bitPack(),
            "double_alp_pfor",
            PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().patchedPFor().bitPack(),
            // Long pipelines
            "long_zstd",
            PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().zstd(),
            "long_xor_bitpack",
            PipelineConfig.forLongs(randomBlockSize()).delta().xor().bitPack(),
            "long_patched_pfor",
            PipelineConfig.forLongs(randomBlockSize()).delta().offset().patchedPFor().bitPack(),
            "long_rle_payload",
            PipelineConfig.forLongs(randomBlockSize()).rlePayload(),
            "long_rle_zstd",
            PipelineConfig.forLongs(randomBlockSize()).rle().zstd()
        );

        final var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)));
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(new Elasticsearch92Lucene103Codec() {

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return formatFromPipelineMap(fieldPipelines);
            }
        });

        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            final int numDocs = randomIntBetween(10, 20) * 8192 + randomIntBetween(1, 8191);
            final long baseTimestamp = 1704067200000L;

            for (int i = 0; i < numDocs; i++) {
                final var d = new Document();
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i));
                d.add(new NumericDocValuesField("double_gorilla", NumericUtils.doubleToSortableLong(20.0 + i * 0.01)));
                d.add(new NumericDocValuesField("double_alp", NumericUtils.doubleToSortableLong(i * 1.5)));
                d.add(new NumericDocValuesField("double_alp_rd", NumericUtils.doubleToSortableLong(Math.sin(i) * 1000.0)));
                d.add(new NumericDocValuesField("double_alp_pfor", NumericUtils.doubleToSortableLong(i * 0.123)));
                d.add(new NumericDocValuesField("long_zstd", i * 100L));
                d.add(new NumericDocValuesField("long_xor_bitpack", baseTimestamp + i * 7L));
                d.add(new NumericDocValuesField("long_patched_pfor", i * 13L + (i % 50 == 0 ? 1_000_000L : 0)));
                d.add(new NumericDocValuesField("long_rle_payload", i / 10L));
                d.add(new NumericDocValuesField("long_rle_zstd", i / 5L));
                d.add(new NumericDocValuesField("long_default", i * 42L));
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                final var leaf = reader.leaves().get(0).reader();

                final String[] fields = {
                    "double_gorilla",
                    "double_alp",
                    "double_alp_rd",
                    "double_alp_pfor",
                    "long_zstd",
                    "long_xor_bitpack",
                    "long_patched_pfor",
                    "long_rle_payload",
                    "long_rle_zstd",
                    "long_default" };
                final var dvs = new NumericDocValues[fields.length];
                for (int f = 0; f < fields.length; f++) {
                    dvs[f] = leaf.getNumericDocValues(fields[f]);
                    assertNotNull("missing doc values for [" + fields[f] + "]", dvs[f]);
                }

                // NOTE: docs are sorted by timestamp descending, so doc[d] corresponds to i = numDocs - 1 - d
                int count = 0;
                while (dvs[0].nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    for (int f = 1; f < dvs.length; f++) {
                        assertEquals(dvs[0].docID(), dvs[f].nextDoc());
                    }

                    final int i = numDocs - 1 - count;
                    assertEquals(20.0 + i * 0.01, NumericUtils.sortableLongToDouble(dvs[0].longValue()), 0.0);
                    assertEquals(i * 1.5, NumericUtils.sortableLongToDouble(dvs[1].longValue()), 0.0);
                    assertEquals(Math.sin(i) * 1000.0, NumericUtils.sortableLongToDouble(dvs[2].longValue()), 0.0);
                    assertEquals(i * 0.123, NumericUtils.sortableLongToDouble(dvs[3].longValue()), 0.0);
                    assertEquals(i * 100L, dvs[4].longValue());
                    assertEquals(baseTimestamp + i * 7L, dvs[5].longValue());
                    assertEquals(i * 13L + (i % 50 == 0 ? 1_000_000L : 0), dvs[6].longValue());
                    assertEquals(i / 10L, dvs[7].longValue());
                    assertEquals(i / 5L, dvs[8].longValue());
                    assertEquals(i * 42L, dvs[9].longValue());
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testPerFieldPipelineSortedNumericRoundTrip() throws Exception {
        final Map<String, PipelineConfig> fieldPipelines = Map.of(
            TIMESTAMP_FIELD,
            PipelineConfig.forLongs(randomBlockSize()).deltaDelta().offset().patchedPFor().bitPack(),
            "double_alp",
            PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().offset().gcd().bitPack(),
            "float_alp",
            PipelineConfig.forFloats(randomBlockSize()).alpFloatStage().offset().gcd().bitPack(),
            "long_delta",
            PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack(),
            "long_pfor",
            PipelineConfig.forLongs(randomBlockSize()).delta().offset().patchedPFor().bitPack()
        );

        final var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)));
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(new Elasticsearch92Lucene103Codec() {

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return formatFromPipelineMap(fieldPipelines);
            }
        });

        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            final int numDocs = randomIntBetween(10, 20) * 8192 + randomIntBetween(1, 8191);
            final long baseTimestamp = 1704067200000L;
            final long interval = 10_000;

            for (int i = 0; i < numDocs; i++) {
                final var d = new Document();
                d.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, baseTimestamp + i * interval));
                d.add(new SortedNumericDocValuesField("double_alp", NumericUtils.doubleToSortableLong(i * 1.5)));
                d.add(new SortedNumericDocValuesField("float_alp", NumericUtils.floatToSortableInt((float) (i * 0.25))));
                d.add(new SortedNumericDocValuesField("long_delta", i * 100L));
                d.add(new SortedNumericDocValuesField("long_pfor", i * 13L + (i % 50 == 0 ? 1_000_000L : 0)));
                d.add(new SortedNumericDocValuesField("long_default", i * 42L));
                // NOTE: add a second value to some docs to exercise the multi-valued path
                if (i % 3 == 0) {
                    d.add(new SortedNumericDocValuesField("long_default", i * 42L + 1));
                }
                iw.addDocument(d);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                final var leaf = reader.leaves().get(0).reader();

                // Verify single-valued sorted numeric fields
                final String[] singleFields = { TIMESTAMP_FIELD, "double_alp", "float_alp", "long_delta", "long_pfor", "long_default" };
                final var dvs = new SortedNumericDocValues[singleFields.length];
                for (int f = 0; f < singleFields.length; f++) {
                    dvs[f] = leaf.getSortedNumericDocValues(singleFields[f]);
                    assertNotNull("missing doc values for [" + singleFields[f] + "]", dvs[f]);
                }

                int count = 0;
                while (dvs[0].nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    for (int f = 1; f < dvs.length; f++) {
                        assertEquals(dvs[0].docID(), dvs[f].nextDoc());
                    }

                    final int i = numDocs - 1 - count;
                    assertEquals(1, dvs[0].docValueCount());
                    assertEquals(baseTimestamp + i * interval, dvs[0].nextValue());
                    assertEquals(1, dvs[1].docValueCount());
                    assertEquals(i * 1.5, NumericUtils.sortableLongToDouble(dvs[1].nextValue()), 0.0);
                    assertEquals(1, dvs[2].docValueCount());
                    assertEquals((float) (i * 0.25), NumericUtils.sortableIntToFloat((int) dvs[2].nextValue()), 0.0f);
                    assertEquals(1, dvs[3].docValueCount());
                    assertEquals(i * 100L, dvs[3].nextValue());
                    assertEquals(1, dvs[4].docValueCount());
                    assertEquals(i * 13L + (i % 50 == 0 ? 1_000_000L : 0), dvs[4].nextValue());

                    final int expectedCount = i % 3 == 0 ? 2 : 1;
                    assertEquals(expectedCount, dvs[5].docValueCount());
                    assertEquals(i * 42L, dvs[5].nextValue());
                    if (expectedCount == 2) {
                        assertEquals(i * 42L + 1, dvs[5].nextValue());
                    }
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    private static Integer randomBlockSize() {
        return randomFrom(128, 256, 512, 1024, 2048, 4096);
    }

    private static ES94TSDBDocValuesFormat formatFromPipelineMap(final Map<String, PipelineConfig> fieldPipelines) {
        return new ES94TSDBDocValuesFormat(
            fieldName -> new PipelineResolver.FieldContext(fieldName, null, null, null, PipelineConfig.DataType.LONG),
            ctx -> {
                final PipelineConfig config = fieldPipelines.get(ctx.fieldName());
                return config != null ? config.blockSize() : ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
            },
            (ctx, sample, sampleSize) -> {
                final PipelineConfig config = fieldPipelines.get(ctx.fieldName());
                return config != null ? config : PipelineConfig.defaultConfig();
            }
        );
    }

}
