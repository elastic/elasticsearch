/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Type;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class ParquetFooterReaderTests extends ESTestCase {

    public void testReadsTheLayoutOfAFlatFile() throws IOException {
        byte[] file = ParquetTestFiles.writeFlat(1_000, 300);
        BytesParquetObject object = new BytesParquetObject("repository://lake/flat.parquet", file);

        ParquetFileInfo info = ParquetFooterReader.read(object);

        assertEquals(1_000, info.rowCount());
        assertEquals(file.length, info.length());
        assertEquals(List.of(300, 300, 300, 100), info.rowGroups().stream().map(ParquetFileInfo.RowGroup::numRows).toList());
        assertEquals(
            List.of("id", "ts", "low", "bucket", "maybe", "name", "kind", "note"),
            info.columns().stream().map(ParquetFileInfo.LeafColumn::path).toList()
        );

        ParquetFileInfo.LeafColumn id = info.column("id");
        assertEquals(Type.INT32.getValue(), id.physicalType());
        assertTrue(id.required());
        assertTrue(id.flat());
        assertFalse(id.repeated());
        assertEquals(Type.INT64.getValue(), info.column("ts").physicalType());
        assertFalse("an optional column is not required", info.column("maybe").required());
        assertEquals(Type.BYTE_ARRAY.getValue(), info.column("name").physicalType());
        assertEquals(ConvertedType.UTF8.getValue(), info.column("name").convertedType());
        assertEquals("a column without an annotation has no converted type", -1, id.convertedType());
        assertNull(info.column("missing"));

        // chunks are inside the file, in order, and every column has one in every row group
        long previousEnd = 4; // after the leading magic
        for (ParquetFileInfo.RowGroup rowGroup : info.rowGroups()) {
            assertEquals(info.columns().size(), rowGroup.chunks().size());
            for (ParquetFileInfo.Chunk chunk : rowGroup.chunks()) {
                assertTrue(chunk.offset() >= previousEnd);
                assertTrue(chunk.compressedSize() > 0);
                assertTrue(chunk.numValues() > 0);
                previousEnd = chunk.offset() + chunk.compressedSize();
                assertTrue(previousEnd <= file.length);
            }
        }
    }

    public void testAFooterCostsOneRequestUnlessItIsLongerThanTheTailThatWeAskFor() throws IOException {
        byte[] file = ParquetTestFiles.writeFlat(1_000, 100);

        BytesParquetObject whole = new BytesParquetObject("repository://lake/flat.parquet", file);
        ParquetFileInfo first = ParquetFooterReader.read(whole);
        assertEquals("the default tail holds the whole footer of a small file", 1, whole.requests.get());

        BytesParquetObject small = new BytesParquetObject("repository://lake/flat.parquet", file);
        ParquetFileInfo second = ParquetFooterReader.read(small, 16);
        assertEquals("a tail that is too short needs a second request for the rest of the footer", 2, small.requests.get());

        assertEquals(first.rowCount(), second.rowCount());
        assertEquals(first.piece(0, 10, 0, List.of("id", "name")), second.piece(0, 10, 0, List.of("id", "name")));
    }

    public void testPieceHoldsTheRowGroupRangeAndTheSnapshotOfTheColumns() throws IOException {
        byte[] file = ParquetTestFiles.writeFlat(1_000, 300);
        ParquetFileInfo info = ParquetFooterReader.read(new BytesParquetObject("repository://lake/flat.parquet", file));

        ExternalFilePiece piece = info.piece(1, 2, 300, List.of("ts", "id"));

        assertEquals(1, piece.firstRowGroup());
        assertArrayEquals(new int[] { 300, 300 }, piece.rowGroupRows());
        assertEquals(600, piece.maxDoc());
        assertEquals(300, piece.indexRowBase());
        assertEquals(List.of("ts", "id"), piece.columns().stream().map(ColumnSnapshot::path).toList());
        ColumnSnapshot ts = piece.columns().get(0);
        assertEquals(2, ts.rowGroups());
        assertEquals(info.rowGroups().get(1).chunks().get(1).offset(), ts.offsets()[0]);
        assertEquals(info.rowGroups().get(2).chunks().get(1).compressedSize(), ts.compressedSizes()[1]);
        assertEquals(300L, ts.numValues()[0]);
        expectThrows(IllegalArgumentException.class, () -> info.piece(3, 2, 0, List.of("id")));
        expectThrows(IllegalArgumentException.class, () -> info.piece(0, 1, 0, List.of("nope")));
    }

    public void testNestedColumnsAreNotFlat() throws IOException {
        ParquetFileInfo info = ParquetFooterReader.read(
            new BytesParquetObject("repository://lake/n.parquet", ParquetTestFiles.writeNested(10))
        );
        assertEquals(List.of("id", "inner.value"), info.columns().stream().map(ParquetFileInfo.LeafColumn::path).toList());
        assertTrue(info.column("id").flat());
        assertFalse(info.column("inner.value").flat());
        assertTrue(info.column("inner.value").required());
    }

    public void testRejectsWhatIsNotAParquetFile() throws IOException {
        byte[] random = randomByteArrayOfLength(randomIntBetween(64, 256));
        random[random.length - 1] = 'X';
        IOException notParquet = expectThrows(
            IOException.class,
            () -> ParquetFooterReader.read(new BytesParquetObject("repository://lake/x", random))
        );
        assertThat(notParquet.getMessage(), org.hamcrest.Matchers.containsString("does not end with the Parquet magic"));

        IOException tooShort = expectThrows(
            IOException.class,
            () -> ParquetFooterReader.read(new BytesParquetObject("repository://lake/x", new byte[5]))
        );
        assertThat(tooShort.getMessage(), org.hamcrest.Matchers.containsString("too short"));

        byte[] file = ParquetTestFiles.writeFlat(10, 10);
        // a footer length that points before the start of the file
        file[file.length - 8] = (byte) 0xFF;
        file[file.length - 7] = (byte) 0xFF;
        file[file.length - 6] = (byte) 0xFF;
        file[file.length - 5] = 0x7F;
        IOException badLength = expectThrows(
            IOException.class,
            () -> ParquetFooterReader.read(new BytesParquetObject("repository://lake/x", file))
        );
        assertThat(badLength.getMessage(), org.hamcrest.Matchers.containsString("invalid footer length"));
    }
}
