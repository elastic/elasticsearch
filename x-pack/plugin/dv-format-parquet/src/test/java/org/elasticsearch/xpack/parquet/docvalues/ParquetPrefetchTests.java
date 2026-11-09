/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;
import java.util.List;

/**
 * A reader that starts a column chunk asks for the next chunk of the column to be fetched in the background, before it reads its own,
 * so that a scan finds each chunk in the cache when it gets there. The object that the reader reads from records the requests.
 */
public class ParquetPrefetchTests extends StubSegmentTestCase {

    private static final List<ColumnBinding> BINDINGS = List.of(
        new ColumnBinding("f.id", 0, "id", DocValuesType.NUMERIC),
        new ColumnBinding("f.name", 4, "name", DocValuesType.BINARY)
    );

    public void testAScanAsksForEachChunkAfterTheFirstOnce() throws IOException {
        int rowGroups = randomIntBetween(2, 8);
        int rowsPerGroup = randomIntBetween(20, 100);
        BytesParquetObject file = new BytesParquetObject(
            FILE,
            ParquetTestFiles.writeFlat(rowGroups * rowsPerGroup, rowsPerGroup, ParquetTestFiles.Options.DEFAULT)
        );
        withSegment(file, BINDINGS, leaf -> {
            NumericDocValues ids = leaf.getNumericDocValues("f.id");
            for (int doc = ids.nextDoc(); doc != NumericDocValues.NO_MORE_DOCS; doc = ids.nextDoc()) {
                assertTrue(ids.longValue() >= 0);
            }
            // the chunk of group g starts the request for group g + 1, so a scan makes one request for each group but the first, and
            // they come in the order of the file
            assertEquals(rowGroups - 1, file.prefetched.size());
            long previous = -1;
            for (long[] range : file.prefetched) {
                assertTrue("in the file", range[0] > previous && range[1] > 0 && range[0] + range[1] <= file.length());
                previous = range[0];
            }
            // another column has its own chunks, so its own requests
            int before = file.prefetched.size();
            BinaryDocValues names = leaf.getBinaryDocValues("f.name");
            while (names.nextDoc() != BinaryDocValues.NO_MORE_DOCS) {
                // reads every chunk of the column
            }
            assertEquals(before + rowGroups - 1, file.prefetched.size());
        });
    }

    public void testAChunkThatIsReadAgainDoesNotAskAgain() throws IOException {
        int rowGroups = 4;
        int rowsPerGroup = 50;
        BytesParquetObject file = new BytesParquetObject(
            FILE,
            ParquetTestFiles.writeFlat(rowGroups * rowsPerGroup, rowsPerGroup, ParquetTestFiles.Options.DEFAULT)
        );
        withSegment(file, BINDINGS, leaf -> {
            NumericDocValues ids = leaf.getNumericDocValues("f.id");
            assertTrue(ids.advanceExact(10));
            assertEquals(1, file.prefetched.size());
            // the same chunk again, and a seek back inside it, which restarts the chunk, do not ask for the next one again
            assertTrue(ids.advanceExact(30));
            assertTrue(ids.advanceExact(5));
            assertEquals(1, file.prefetched.size());
            assertTrue(ids.advanceExact(rowGroups * rowsPerGroup - 1));
            assertEquals("the last group has no group after it, and a jump over groups asks for nothing", 1, file.prefetched.size());
        });
    }

    public void testAnObjectThatDoesNotPrefetchChangesNothing() throws IOException {
        // the default of ParquetObject#prefetchRange does nothing, which is what an object without a cache has
        ParquetObject plain = new ParquetObject() {
            @Override
            public String location() {
                return FILE;
            }

            @Override
            public long length() {
                return 0;
            }

            @Override
            public byte[] readRange(long position, int length) {
                throw new AssertionError("a prefetch must not read");
            }
        };
        plain.prefetchRange(0, 10);
    }
}
