/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A file whose last line has no trailing newline still ends with a complete record, and a read that
 * owns the file's end must return it.
 *
 * <p>Dropping a trailing partial record is correct only for a split that is <em>not</em> the file's
 * last — the next split re-reads those bytes. A whole-file read has no next split, so the same
 * behaviour silently loses the final row while {@code COUNT(*)} still counts it. These tests state
 * the contract from the reader's side; whether a given split is actually marked as the file's last is
 * decided by the split producer and pinned in {@code FileSplitProviderTests}.
 */
public class NdJsonUnterminatedRecordTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testSingleRecordWithNoTrailingNewlineIsRead() throws Exception {
        assertIds("{\"id\":1}", List.of(1));
    }

    public void testLastRecordWithNoTrailingNewlineIsRead() throws Exception {
        assertIds("{\"id\":1}\n{\"id\":2}\n{\"id\":3}", List.of(1, 2, 3));
    }

    public void testTrailingNewlineDoesNotYieldAPhantomRecord() throws Exception {
        assertIds("{\"id\":1}\n{\"id\":2}\n", List.of(1, 2));
    }

    public void testCarriageReturnTerminatedLastLineIsRead() throws Exception {
        assertIds("{\"id\":1}\r\n{\"id\":2}\r\n", List.of(1, 2));
    }

    public void testCarriageReturnTerminatorWithUnterminatedLastRecord() throws Exception {
        assertIds("{\"id\":1}\r\n{\"id\":2}", List.of(1, 2));
    }

    /**
     * The other side of the contract: a split that is not the file's last drops its trailing partial
     * record, because the following split re-reads those bytes. Marking a whole-file read this way is
     * what lost the final row.
     */
    public void testNonLastSplitStillDropsItsTrailingPartialRecord() throws Exception {
        List<Integer> ids = readIds("{\"id\":1}\n{\"id\":2}", false);
        assertEquals("a non-last split leaves its trailing partial record to the next split", List.of(1), ids);
    }

    private void assertIds(String ndjson, List<Integer> expected) throws Exception {
        assertEquals(expected, readIds(ndjson, true));
    }

    private List<Integer> readIds(String ndjson, boolean lastSplit) throws Exception {
        StorageObject object = new BytesStorageObject("file:///eof.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(10)
            .firstSplit(true)
            .lastSplit(lastSplit)
            .recordAligned(false)
            .build();

        List<Integer> ids = new ArrayList<>();
        try (CloseableIterator<Page> pages = new NdJsonFormatReader(null, blockFactory).read(object, context)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    IntBlock block = (IntBlock) page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        ids.add(block.getInt(i));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return ids;
    }
}
