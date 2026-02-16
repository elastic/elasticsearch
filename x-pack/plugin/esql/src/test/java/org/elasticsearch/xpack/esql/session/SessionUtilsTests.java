/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.unit.ByteSizeUnit.GB;
import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.session.SessionUtils.checkPagesBelowSize;
import static org.elasticsearch.xpack.esql.session.SessionUtils.fromPages;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SessionUtilsTests extends ESTestCase {

    private final BlockFactory BLOCK_FACTORY_1GB = blockFactory((int) GB.toBytes(1));
    private final Attribute KEYWORD_ATTRIBUTE = new ReferenceAttribute(
        randomSource(),
        randomAlphaOfLengthOrNull(10),
        randomAlphaOfLength(10),
        DataType.KEYWORD
    );

    record PagesRec(List<Page> pages, byte[] data, int dataLen, int totalRows) implements Releasable {
        @Override
        public void close() {
            Releasables.close(pages);
        }
    }

    public void testFromPages() {
        try (PagesRec pagesRec = generatePageSet(BLOCK_FACTORY_1GB)) {
            Block[] outBlocks = fromPages(List.of(KEYWORD_ATTRIBUTE), pagesRec.pages, BLOCK_FACTORY_1GB);

            assertThat(outBlocks.length, is(1));
            // Verify that the resulted "compacted" block contains the same number of rows
            BytesRefBlock bytesBlock = (BytesRefBlock) outBlocks[0];
            assertThat(bytesBlock.getPositionCount(), is(pagesRec.totalRows));

            // Verify that the resulting BytesRefBlock contains the same bytes from the input Pages.
            byte[] outBuffer = new byte[pagesRec.dataLen];
            for (int i = 0, posCount = bytesBlock.getPositionCount(), outOffset = 0; i < posCount; i++) {
                BytesRef ref = bytesBlock.getBytesRef(i, new BytesRef());
                System.arraycopy(ref.bytes, ref.offset, outBuffer, outOffset, ref.length);
                outOffset += ref.length;
            }
            assertThat(outBuffer, is(pagesRec.data));

            Releasables.close(outBlocks);
        }
    }

    public void testFromPagesCircuitBreaks() {
        try (PagesRec pagesRec = generatePageSet(BLOCK_FACTORY_1GB)) {
            BlockFactory convertBlockFactory = blockFactory(pagesRec.dataLen - 1);
            assertThrows(CircuitBreakingException.class, () -> fromPages(List.of(KEYWORD_ATTRIBUTE), pagesRec.pages, convertBlockFactory));
        }
    }

    public void testCheckPagesBelowSize() {
        try (PagesRec pagesRec = generatePageSet(BLOCK_FACTORY_1GB)) {
            var message = "data too large: ";
            var ex = assertThrows(
                IllegalArgumentException.class,
                () -> checkPagesBelowSize(pagesRec.pages, ByteSizeValue.ofBytes(pagesRec.dataLen - 1), l -> message + l)
            );
            // pages are mocked, their size is considerably larger than dataLen
            long pagesRamSize = pagesRec.pages.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
            assertThat(ex.getMessage(), containsString(message + pagesRamSize));
        }
    }

    private static PagesRec generatePageSet(BlockFactory blockFactory) {
        final int minBytes = 500;
        final int maxBytes = randomIntBetween(minBytes, minBytes * 1_000);
        return generatePages(minBytes, maxBytes, blockFactory);
    }

    // Generates a list of Pages with one BytesRef block, each of different positions, filled with random bytes.
    private static PagesRec generatePages(int minBytes, int maxBytes, BlockFactory blockFactory) {
        BytesRefBlock.Builder builder = null;
        try {
            builder = blockFactory.newBytesRefBlockBuilder(maxBytes);

            byte[] buffer = new byte[maxBytes];
            List<Page> pages = new ArrayList<>();

            int producedBytes = 0;
            int producedRows = 0;
            int rowsPerPage = randomIntBetween(1, 100);
            int rows = 0;
            while (producedBytes < maxBytes) {
                int rowBytes = Math.min(randomIntBetween(1, maxBytes / minBytes), maxBytes - producedBytes);
                byte[] rowValue = randomByteArrayOfLength(rowBytes);

                builder.appendBytesRef(new BytesRef(rowValue));
                System.arraycopy(rowValue, 0, buffer, producedBytes, rowBytes);

                producedBytes += rowBytes;
                rows++;

                if (rows > rowsPerPage) {
                    producedRows += rows;
                    rows = 0;
                    enqueueBlock(builder, pages);
                    Releasables.close(builder);
                    builder = blockFactory.newBytesRefBlockBuilder(maxBytes);
                    rowsPerPage = randomIntBetween(1, 100);
                }
            }
            if (rows > 0) {
                producedRows += rows;
                enqueueBlock(builder, pages);
            }

            return new PagesRec(pages, buffer, producedBytes, producedRows);
        } finally {
            Releasables.close(builder);
        }
    }

    private BlockFactory blockFactory(long maxBytes) {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(this.getClass().getName(), ByteSizeValue.ofBytes(maxBytes));
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
        return new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
    }

    private static void enqueueBlock(BytesRefBlock.Builder builder, List<Page> pages) {
        Block block = builder.build();
        pages.add(new Page(block));
    }
}
