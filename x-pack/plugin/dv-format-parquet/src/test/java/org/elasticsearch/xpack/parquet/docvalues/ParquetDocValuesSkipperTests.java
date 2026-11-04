/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The doc values skipper that a stub segment serves from the footer statistics: one interval for each row group, with the range and
 * the number of values of the group, the conversions applied, and the same answers from range queries as a scan gives.
 */
public class ParquetDocValuesSkipperTests extends StubSegmentTestCase {

    private static final int ROWS = 2_500;
    private static final int ROW_GROUP_ROWS = 1_000;

    private static BytesParquetObject flatFile() throws IOException {
        return new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(ROWS, ROW_GROUP_ROWS));
    }

    public void testEveryRowGroupIsOneIntervalWithItsRangeAndItsValueCount() throws IOException {
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.maybe", 1, "maybe", DocValuesType.NUMERIC)
        );
        withSegment(flatFile(), bindings, leaf -> {
            DocValuesSkipper skipper = leaf.getDocValuesSkipper("parquet.id");
            assertNotNull(skipper);
            assertEquals(1, skipper.numLevels());
            assertEquals(0L, skipper.minValue());
            assertEquals(ROWS - 1L, skipper.maxValue());
            assertEquals(ROWS, skipper.docCount());
            assertEquals(-1, skipper.minDocID(0));

            skipper.advance(0);
            assertInterval(skipper, 0, 999, 0, 999, 1_000);
            skipper.advance(1_500);
            assertInterval(skipper, 1_000, 1_999, 1_000, 1_999, 1_000);
            skipper.advance(2_499);
            assertInterval(skipper, 2_000, 2_499, 2_000, 2_499, 500);
            skipper.advance(2_500);
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));

            // an optional column counts the rows that have a value: every row but the multiples of 3, and the values are i % 13
            DocValuesSkipper optional = leaf.getDocValuesSkipper("parquet.maybe");
            assertNotNull(optional);
            int expected = 0;
            for (int i = 0; i < ROWS; i++) {
                expected += i % 3 != 0 ? 1 : 0;
            }
            assertEquals(expected, optional.docCount());
            optional.advance(0);
            int firstGroup = 0;
            for (int i = 0; i < ROW_GROUP_ROWS; i++) {
                firstGroup += i % 3 != 0 ? 1 : 0;
            }
            assertEquals(firstGroup, optional.docCount(0));
            assertEquals(0, optional.minDocID(0));
            assertEquals(ROW_GROUP_ROWS - 1, optional.maxDocID(0));
            assertTrue(optional.minValue() >= 0 && optional.maxValue() <= 12);
        });
    }

    public void testTheRangeFollowsTheConversionOfTheField() throws IOException {
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.ts", 0, "ts", DocValuesType.NUMERIC, Conversion.SECONDS_TO_MILLIS)
        );
        withSegment(flatFile(), bindings, leaf -> {
            DocValuesSkipper skipper = leaf.getDocValuesSkipper("parquet.ts");
            // ts holds i * 1000 seconds, which are i * 1,000,000 milliseconds
            assertEquals(0L, skipper.minValue());
            assertEquals((ROWS - 1L) * 1_000_000L, skipper.maxValue());
            skipper.advance(1_000);
            assertEquals(1_000_000_000L, skipper.minValue(0));
            assertEquals(1_999_000_000L, skipper.maxValue(0));
        });
    }

    public void testUnsignedColumnsUseTheUnsignedOrder() throws IOException {
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeUnsigned(50));
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.u", 0, "u", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.small", 1, "small", DocValuesType.NUMERIC)
        );
        withSegment(file, bindings, leaf -> {
            DocValuesSkipper unsigned = leaf.getDocValuesSkipper("parquet.u");
            assertEquals("UINT_32 keeps all 32 bits", 1L << 31, unsigned.minValue());
            assertEquals((1L << 31) + 49, unsigned.maxValue());
            DocValuesSkipper small = leaf.getDocValuesSkipper("parquet.small");
            assertEquals(65_000L, small.minValue());
            assertEquals(65_049L, small.maxValue());
        });
    }

    /** A range query over a stub segment finds the same documents as a scan, whatever the skipper says about the groups. */
    public void testRangeQueriesGiveTheSameAnswerAsAScan() throws IOException {
        List<ColumnBinding> bindings = List.of(new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC));
        withSegment(flatFile(), bindings, leaf -> {
            IndexSearcher searcher = new IndexSearcher(leaf);
            for (int i = 0; i < 20; i++) {
                long lower = randomLongBetween(-100, ROWS + 100);
                long upper = randomLongBetween(lower, ROWS + 200);
                long expected = Math.max(0, Math.min(upper, ROWS - 1) - Math.max(lower, 0) + 1);
                assertEquals(
                    "[" + lower + ", " + upper + "]",
                    expected,
                    searcher.count(NumericDocValuesField.newSlowRangeQuery("parquet.id", lower, upper))
                );
            }
            assertEquals(0, searcher.count(NumericDocValuesField.newSlowRangeQuery("parquet.id", ROWS, ROWS + 10)));
            assertEquals(ROWS, searcher.count(NumericDocValuesField.newSlowRangeQuery("parquet.id", -10, ROWS + 10)));
        });
    }

    /** A column whose null counts are not in the footer gets no skip index, because its skipper could not tell the value count. */
    public void testNoSkipIndexWithoutValueCounts() throws IOException {
        BytesParquetObject file = flatFile();
        ParquetFileInfo footer = ParquetFooterReader.read(file);
        ExternalFilePiece piece = footer.piece(0, footer.rowGroups().size(), 0, List.of("id", "maybe"));
        List<ColumnSnapshot> columns = new ArrayList<>();
        for (ColumnSnapshot column : piece.columns()) {
            columns.add(
                column.path().equals("maybe")
                    ? new ColumnSnapshot(
                        column.path(),
                        column.physicalType(),
                        column.convertedType(),
                        column.required(),
                        column.codec(),
                        column.offsets(),
                        column.compressedSizes(),
                        column.uncompressedSizes(),
                        column.numValues()
                    )
                    : column
            );
        }
        ExternalFilePiece withoutCounts = new ExternalFilePiece(
            piece.location(),
            piece.length(),
            piece.indexRowBase(),
            piece.firstRowGroup(),
            piece.rowGroupRows(),
            columns
        );
        ExternalParquetCodecReader reader = new ExternalParquetCodecReader(
            List.of(
                new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC),
                new ColumnBinding("parquet.maybe", 1, "maybe", DocValuesType.NUMERIC)
            ),
            List.of(withoutCounts)
        );
        assertEquals(DocValuesSkipIndexType.RANGE, reader.getFieldInfos().fieldInfo("parquet.id").docValuesSkipIndexType());
        assertEquals(DocValuesSkipIndexType.NONE, reader.getFieldInfos().fieldInfo("parquet.maybe").docValuesSkipIndexType());
    }

    private static void assertInterval(DocValuesSkipper skipper, int minDoc, int maxDoc, long min, long max, int docCount) {
        assertEquals(minDoc, skipper.minDocID(0));
        assertEquals(maxDoc, skipper.maxDocID(0));
        assertEquals(min, skipper.minValue(0));
        assertEquals(max, skipper.maxValue(0));
        assertEquals(docCount, skipper.docCount(0));
    }
}
