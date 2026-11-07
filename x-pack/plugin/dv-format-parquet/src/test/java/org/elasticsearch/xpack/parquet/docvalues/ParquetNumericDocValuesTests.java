/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The decoder against real Parquet files that the tests write: every value of a column, in order and by random seeks, over row groups
 * and pages, with dictionary and plain pages, Snappy and no compression.
 */
public class ParquetNumericDocValuesTests extends StubSegmentTestCase {

    private static List<ColumnBinding> idAndTs() {
        return List.of(
            new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.ts", 1, "ts", DocValuesType.NUMERIC)
        );
    }

    private static void assertEveryValueInOrder(LeafReader leaf, int rows) throws IOException {
        NumericDocValues id = leaf.getNumericDocValues("parquet.id");
        NumericDocValues ts = leaf.getNumericDocValues("parquet.ts");
        for (int doc = 0; doc < rows; doc++) {
            assertEquals(doc, id.nextDoc());
            assertEquals(doc, ts.nextDoc());
            assertEquals(doc, id.longValue());
            assertEquals(doc * 1000L, ts.longValue());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, id.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, ts.nextDoc());
    }

    private static void assertRandomSeeks(LeafReader leaf, int rows) throws IOException {
        NumericDocValues id = leaf.getNumericDocValues("parquet.id");
        NumericDocValues ts = leaf.getNumericDocValues("parquet.ts");
        Set<Integer> targets = new HashSet<>();
        int count = Math.min(rows, randomIntBetween(10, 200));
        while (targets.size() < count) {
            targets.add(randomIntBetween(0, rows - 1));
        }
        int[] sorted = targets.stream().mapToInt(Integer::intValue).sorted().toArray();
        for (int target : sorted) {
            if (randomBoolean()) {
                assertTrue(id.advanceExact(target));
            } else {
                assertEquals(target, id.advance(target));
            }
            assertEquals(target, id.docID());
            assertEquals(target, id.longValue());
            assertTrue(ts.advanceExact(target));
            assertEquals(target * 1000L, ts.longValue());
        }
        // a seek backward restarts the chunk and still gives the right value
        assertTrue(id.advanceExact(sorted[0]));
        assertEquals(sorted[0], id.longValue());
        assertFalse(id.advanceExact(rows));
    }

    public void testEveryCombinationOfWriterOptions() throws IOException {
        int rows = randomIntBetween(1, 2_500);
        int rowGroupRows = randomIntBetween(1, 1_000);
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 256, 1024, 64 * 1024),
            randomFrom(64, 512, 1024 * 1024),
            randomBoolean() ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED
        );
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, rowGroupRows, options));
        logger.info("--> [{}] rows in row groups of [{}] with {}", rows, rowGroupRows, options);
        List<ColumnBinding> bindings = new ArrayList<>(idAndTs());
        bindings.add(new ColumnBinding("parquet.low", 2, "low", DocValuesType.NUMERIC));
        withSegment(file, bindings, leaf -> {
            assertEquals(rows, leaf.maxDoc());
            assertEveryValueInOrder(leaf, rows);
            assertRandomSeeks(leaf, rows);
            NumericDocValues low = leaf.getNumericDocValues("parquet.low");
            for (int doc = 0; doc < rows; doc++) {
                assertEquals(doc, low.nextDoc());
                assertEquals(doc % 7, low.longValue());
            }
        });
    }

    public void testDictionaryPagesFallBackToPlainPagesInOneChunk() throws IOException {
        // a dictionary of 2 KB holds about 500 distinct ints: the writer starts with dictionary pages for the repeated values of the
        // first half, and continues with plain pages once the distinct values of the second half fill the dictionary
        int rows = 4_000;
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(true, 128, 2048, CompressionCodecName.SNAPPY);
        byte[] bytes = ParquetTestFiles.writeMixedCardinality(rows, options);
        BytesParquetObject file = new BytesParquetObject(FILE, bytes);

        ParquetFileInfo footer = ParquetFooterReader.read(file);
        ColumnSnapshot v = footer.piece(0, 1, 0, List.of("v")).columns().get(0);
        Set<String> encodings = pageEncodings(bytes, v, 0);
        assertTrue("the chunk must have both kinds of pages: " + encodings, encodings.containsAll(List.of("PLAIN_DICTIONARY", "PLAIN")));

        withSegment(file, List.of(new ColumnBinding("parquet.v", 0, "v", DocValuesType.NUMERIC)), leaf -> {
            NumericDocValues values = leaf.getNumericDocValues("parquet.v");
            for (int doc = 0; doc < rows; doc++) {
                assertEquals(doc, values.nextDoc());
                assertEquals("row " + doc, ParquetTestFiles.mixedValue(doc, rows), values.longValue());
            }
        });
    }

    public void testLowCardinalityColumnsStayDictionaryEncoded() throws IOException {
        byte[] bytes = ParquetTestFiles.writeFlat(
            2_000,
            700,
            new ParquetTestFiles.Options(true, 256, 1024 * 1024, CompressionCodecName.SNAPPY)
        );
        BytesParquetObject file = new BytesParquetObject(FILE, bytes);
        ParquetFileInfo footer = ParquetFooterReader.read(file);
        ExternalFilePiece piece = footer.piece(0, 3, 0, List.of("low", "bucket"));
        for (ColumnSnapshot column : piece.columns()) {
            assertEquals(column.path(), Set.of("PLAIN_DICTIONARY"), pageEncodings(bytes, column, 0));
        }
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.low", 0, "low", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.bucket", 1, "bucket", DocValuesType.NUMERIC)
        );
        withSegment(file, bindings, leaf -> {
            NumericDocValues low = leaf.getNumericDocValues("parquet.low");
            NumericDocValues bucket = leaf.getNumericDocValues("parquet.bucket");
            for (int doc = 0; doc < 2_000; doc++) {
                assertEquals(doc, low.nextDoc());
                assertEquals(doc % 7, low.longValue());
                assertEquals(doc, bucket.nextDoc());
                assertEquals((doc % 11) * 10_000_000_000L, bucket.longValue());
            }
        });
    }

    public void testRowGroupBordersAndSmallPages() throws IOException {
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(false, 64, 1024, CompressionCodecName.UNCOMPRESSED);
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(1_000, 100, options));
        withSegment(file, idAndTs(), leaf -> {
            NumericDocValues id = leaf.getNumericDocValues("parquet.id");
            // the last row of a row group and the first row of the next one
            for (int border = 100; border < 1_000; border += 100) {
                assertTrue(id.advanceExact(border - 1));
                assertEquals(border - 1, id.longValue());
                assertTrue(id.advanceExact(border));
                assertEquals(border, id.longValue());
            }
        });
    }

    public void testUnsignedIntegersAreNotSignExtended() throws IOException {
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeUnsigned(50));
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.u", 0, "u", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.small", 1, "small", DocValuesType.NUMERIC)
        );
        withSegment(file, bindings, leaf -> {
            NumericDocValues unsigned = leaf.getNumericDocValues("parquet.u");
            NumericDocValues small = leaf.getNumericDocValues("parquet.small");
            for (int doc = 0; doc < 50; doc++) {
                assertEquals(doc, unsigned.nextDoc());
                assertEquals("UINT_32 keeps all 32 bits", (1L << 31) + doc, unsigned.longValue());
                assertEquals(doc, small.nextDoc());
                assertEquals(65_000L + doc, small.longValue());
            }
        });
    }

    public void testReadingNeedsTheFilesOfTheEngine() throws IOException {
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(10, 10));
        ParquetFileInfo footer = ParquetFooterReader.read(file);
        ExternalFilePiece piece = footer.piece(0, 1, 0, List.of("id"));
        try (Directory dir = ParquetTestFiles.newStubDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(new ParquetAttachCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                writer.addIndexes(
                    new ExternalParquetCodecReader(List.of(new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC)), List.of(piece))
                );
                writer.commit();
            }
            // opened outside of the context of the engine: the segment can be described, but not read
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(10, reader.maxDoc());
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> reader.leaves().get(0).reader().getNumericDocValues("parquet.id")
                );
                assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("not opened by an engine"));
            }
        }
    }

    public void testNestedColumnsAndUnsupportedTypesAreRejectedWhenTheyAreBound() throws IOException {
        ParquetFileInfo flat = ParquetFooterReader.read(new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(10, 10)));
        // optional columns and strings are served now
        var optional = org.elasticsearch.common.settings.Settings.builder()
            .put(ParquetBackedSettings.FIELDS.getKey(), "m:maybe,n:name")
            .build();
        assertEquals(
            List.of(DocValuesType.NUMERIC, DocValuesType.BINARY),
            ParquetBackedSettings.bindings(optional, flat).stream().map(ColumnBinding::type).toList()
        );

        ParquetFileInfo nested = ParquetFooterReader.read(new BytesParquetObject(FILE, ParquetTestFiles.writeNested(5)));
        var inner = org.elasticsearch.common.settings.Settings.builder()
            .put(ParquetBackedSettings.FIELDS.getKey(), "v:inner.value")
            .build();
        IllegalArgumentException notFlat = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetBackedSettings.bindings(inner, nested)
        );
        assertThat(notFlat.getMessage(), org.hamcrest.Matchers.containsString("nested"));
    }

    public void testConversionsMakeTheValuesThatTheMappedFieldTypeExpects() throws IOException {
        // ts is id * 1000 (epoch seconds), low is id % 7 (days) and bucket is (id % 11) * 10^10 (micros): the last two are dictionary
        // encoded, so the conversion is applied to the distinct values of the dictionary
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(500, 200));
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("f.millis", 0, "ts", DocValuesType.NUMERIC, Conversion.SECONDS_TO_MILLIS),
            new ColumnBinding("f.days", 1, "low", DocValuesType.NUMERIC, Conversion.DAYS_TO_MILLIS),
            new ColumnBinding("f.micros_to_millis", 2, "bucket", DocValuesType.NUMERIC, Conversion.MICROS_TO_MILLIS),
            new ColumnBinding("f.micros_to_nanos", 3, "bucket", DocValuesType.NUMERIC, Conversion.MICROS_TO_NANOS)
        );
        withSegment(file, bindings, leaf -> {
            NumericDocValues millis = leaf.getNumericDocValues("f.millis");
            NumericDocValues days = leaf.getNumericDocValues("f.days");
            NumericDocValues toMillis = leaf.getNumericDocValues("f.micros_to_millis");
            NumericDocValues toNanos = leaf.getNumericDocValues("f.micros_to_nanos");
            for (int doc = 0; doc < 500; doc++) {
                assertEquals(doc, millis.nextDoc());
                assertEquals(doc * 1_000_000L, millis.longValue());
                assertEquals(doc, days.nextDoc());
                assertEquals((doc % 7) * 86_400_000L, days.longValue());
                assertEquals(doc, toMillis.nextDoc());
                assertEquals((doc % 11) * 10_000_000L, toMillis.longValue());
                assertEquals(doc, toNanos.nextDoc());
                assertEquals((doc % 11) * 10_000_000_000_000L, toNanos.longValue());
            }
        });
    }

    public void testConversionsAreParsedFromTheFieldSettingAndNeedAnIntegerColumn() throws IOException {
        ParquetFileInfo flat = ParquetFooterReader.read(new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(10, 10)));
        var settings = org.elasticsearch.common.settings.Settings.builder()
            .put(ParquetBackedSettings.FIELDS.getKey(), "t:ts:seconds_to_millis, id:id")
            .build();
        List<ColumnBinding> bindings = ParquetBackedSettings.bindings(settings, flat);
        assertEquals(Conversion.SECONDS_TO_MILLIS, bindings.get(0).conversion());
        assertEquals(Conversion.NONE, bindings.get(1).conversion());

        var text = org.elasticsearch.common.settings.Settings.builder()
            .put(ParquetBackedSettings.FIELDS.getKey(), "n:name:days_to_millis")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ParquetBackedSettings.bindings(text, flat));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("needs an integer column"));
        var unknown = org.elasticsearch.common.settings.Settings.builder()
            .put(ParquetBackedSettings.FIELDS.getKey(), "t:ts:fortnights")
            .build();
        expectThrows(IllegalArgumentException.class, () -> ParquetBackedSettings.bindings(unknown, flat));
    }
}
