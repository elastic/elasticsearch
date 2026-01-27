/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link BatchPageSorterOperator}.
 */
public class BatchPageSorterOperatorTests extends ESTestCase {

    private DriverContext driverContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        driverContext = new DriverContext(bigArrays, blockFactory, LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS);
    }

    /**
     * Test that pages arriving in order are passed through immediately.
     */
    public void testPagesInOrder() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            List<BatchPage> outputPages = new ArrayList<>();

            // Send pages in order: 0, 1, 2 (last)
            for (int i = 0; i < 3; i++) {
                BatchPage page = createBatchPage(0, i, i == 2);
                sorter.addInput(page);

                // Page should be immediately available
                Page output = sorter.getOutput();
                assertNotNull("Page " + i + " should be output immediately", output);
                outputPages.add((BatchPage) output);
            }

            // Verify all pages were output in order
            assertThat(outputPages.size(), equalTo(3));
            for (int i = 0; i < 3; i++) {
                assertThat(outputPages.get(i).pageIndexInBatch(), equalTo(i));
            }

            // No more output
            assertNull(sorter.getOutput());

            // Clean up
            for (BatchPage page : outputPages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Test that out-of-order pages are buffered and output in correct order.
     */
    public void testPagesOutOfOrder() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            List<BatchPage> outputPages = new ArrayList<>();

            // Send pages out of order: 2 (last), 0, 1
            BatchPage page2 = createBatchPage(0, 2, true);
            sorter.addInput(page2);
            assertNull("Page 2 should be buffered (waiting for 0)", sorter.getOutput());

            BatchPage page0 = createBatchPage(0, 0, false);
            sorter.addInput(page0);
            // Page 0 should be output now
            Page output0 = sorter.getOutput();
            assertNotNull("Page 0 should be output", output0);
            outputPages.add((BatchPage) output0);
            // Page 2 still buffered (waiting for 1)
            assertNull("Page 2 should still be buffered", sorter.getOutput());

            BatchPage page1 = createBatchPage(0, 1, false);
            sorter.addInput(page1);
            // Pages 1 and 2 should now be available
            Page output1 = sorter.getOutput();
            assertNotNull("Page 1 should be output", output1);
            outputPages.add((BatchPage) output1);

            Page output2 = sorter.getOutput();
            assertNotNull("Page 2 should be output", output2);
            outputPages.add((BatchPage) output2);

            // Verify order
            assertThat(outputPages.size(), equalTo(3));
            assertThat(outputPages.get(0).pageIndexInBatch(), equalTo(0));
            assertThat(outputPages.get(1).pageIndexInBatch(), equalTo(1));
            assertThat(outputPages.get(2).pageIndexInBatch(), equalTo(2));

            // Verify last page marker preserved
            assertThat(outputPages.get(2).isLastPageInBatch(), equalTo(true));

            // No more output
            assertNull(sorter.getOutput());

            // Clean up
            for (BatchPage page : outputPages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Test multiple batches with interleaved pages.
     */
    public void testMultipleBatchesInterleaved() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            List<BatchPage> outputPages = new ArrayList<>();

            // Batch 0: pages 0, 1
            // Batch 1: pages 0, 1
            // Send interleaved: batch0-page1, batch1-page0, batch0-page0, batch1-page1

            sorter.addInput(createBatchPage(0, 1, true)); // batch0, page1 (last) - buffered
            assertNull(sorter.getOutput());

            sorter.addInput(createBatchPage(1, 0, false)); // batch1, page0 - output
            outputPages.add((BatchPage) sorter.getOutput());
            assertNull(sorter.getOutput());

            sorter.addInput(createBatchPage(0, 0, false)); // batch0, page0 - output, then page1 flushes
            outputPages.add((BatchPage) sorter.getOutput()); // batch0, page0
            outputPages.add((BatchPage) sorter.getOutput()); // batch0, page1
            assertNull(sorter.getOutput());

            sorter.addInput(createBatchPage(1, 1, true)); // batch1, page1 (last) - output
            outputPages.add((BatchPage) sorter.getOutput());
            assertNull(sorter.getOutput());

            // Verify we got 4 pages
            assertThat(outputPages.size(), equalTo(4));

            // Verify batch 0 pages are in order
            List<BatchPage> batch0Pages = outputPages.stream().filter(p -> p.batchId() == 0).toList();
            assertThat(batch0Pages.size(), equalTo(2));
            assertThat(batch0Pages.get(0).pageIndexInBatch(), equalTo(0));
            assertThat(batch0Pages.get(1).pageIndexInBatch(), equalTo(1));
            assertThat(batch0Pages.get(1).isLastPageInBatch(), equalTo(true));

            // Verify batch 1 pages are in order
            List<BatchPage> batch1Pages = outputPages.stream().filter(p -> p.batchId() == 1).toList();
            assertThat(batch1Pages.size(), equalTo(2));
            assertThat(batch1Pages.get(0).pageIndexInBatch(), equalTo(0));
            assertThat(batch1Pages.get(1).pageIndexInBatch(), equalTo(1));
            assertThat(batch1Pages.get(1).isLastPageInBatch(), equalTo(true));

            // Clean up
            for (BatchPage page : outputPages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Test that marker pages (empty pages with isLastPageInBatch=true) are handled correctly.
     */
    public void testMarkerPage() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            // Send a marker page for an empty batch
            BatchPage marker = BatchPage.createMarker(0, 0);
            sorter.addInput(marker);

            Page output = sorter.getOutput();
            assertNotNull("Marker should be output", output);
            assertThat(((BatchPage) output).isBatchMarkerOnly(), equalTo(true));
            assertThat(((BatchPage) output).pageIndexInBatch(), equalTo(0));

            assertNull(sorter.getOutput());
        }
    }

    /**
     * Test canProduceMoreDataWithoutExtraInput returns correct values.
     */
    public void testCanProduceMoreData() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            assertThat(sorter.canProduceMoreDataWithoutExtraInput(), equalTo(false));

            // Add page 1 (buffered, waiting for 0)
            sorter.addInput(createBatchPage(0, 1, true));
            assertThat(sorter.canProduceMoreDataWithoutExtraInput(), equalTo(false));

            // Add page 0 (should flush both)
            sorter.addInput(createBatchPage(0, 0, false));
            assertThat(sorter.canProduceMoreDataWithoutExtraInput(), equalTo(true));

            // Drain output
            BatchPage p1 = (BatchPage) sorter.getOutput();
            assertThat(sorter.canProduceMoreDataWithoutExtraInput(), equalTo(true));
            BatchPage p2 = (BatchPage) sorter.getOutput();
            assertThat(sorter.canProduceMoreDataWithoutExtraInput(), equalTo(false));

            p1.releaseBlocks();
            p2.releaseBlocks();
        }
    }

    /**
     * Test isFinished behavior.
     */
    public void testIsFinished() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            assertThat(sorter.isFinished(), equalTo(false));

            sorter.finish();
            // Still not finished if there's buffered data
            assertThat(sorter.isFinished(), equalTo(true));

            // Add and drain a page
            BatchPageSorterOperator sorter2 = new BatchPageSorterOperator();
            sorter2.addInput(createBatchPage(0, 0, true));
            sorter2.finish();
            assertThat(sorter2.isFinished(), equalTo(false)); // has output

            BatchPage output = (BatchPage) sorter2.getOutput();
            assertThat(sorter2.isFinished(), equalTo(true));

            output.releaseBlocks();
            sorter2.close();
        }
    }

    /**
     * Test that close releases buffered pages.
     */
    public void testCloseReleasesBufferedPages() {
        BatchPageSorterOperator sorter = new BatchPageSorterOperator();

        // Add out-of-order pages that will be buffered
        sorter.addInput(createBatchPage(0, 2, true));
        sorter.addInput(createBatchPage(0, 1, false));
        // Page 0 missing, so pages 1 and 2 are buffered

        // Drain page that was output (none should be output since page 0 is missing)
        assertNull(sorter.getOutput());

        // Close should release buffered pages without error
        sorter.close();
    }

    /**
     * Test reverse order arrival (worst case).
     */
    public void testReverseOrderArrival() {
        try (BatchPageSorterOperator sorter = new BatchPageSorterOperator()) {
            List<BatchPage> outputPages = new ArrayList<>();
            int numPages = 5;

            // Send pages in reverse order: 4, 3, 2, 1, 0
            for (int i = numPages - 1; i >= 0; i--) {
                sorter.addInput(createBatchPage(0, i, i == numPages - 1));
            }

            // All pages should now be available in correct order
            for (int i = 0; i < numPages; i++) {
                Page output = sorter.getOutput();
                assertNotNull("Page " + i + " should be available", output);
                outputPages.add((BatchPage) output);
            }

            assertNull(sorter.getOutput());

            // Verify order
            for (int i = 0; i < numPages; i++) {
                assertThat(outputPages.get(i).pageIndexInBatch(), equalTo(i));
            }
            assertThat(outputPages.get(numPages - 1).isLastPageInBatch(), equalTo(true));

            // Clean up
            for (BatchPage page : outputPages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Helper to create a BatchPage with test data.
     */
    private BatchPage createBatchPage(long batchId, int pageIndex, boolean isLast) {
        IntBlock dataBlock = driverContext.blockFactory().newIntBlockBuilder(1).appendInt((int) (batchId * 100 + pageIndex)).build();
        Page dataPage = new Page(dataBlock);
        return new BatchPage(dataPage, batchId, pageIndex, isLast);
    }
}
