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
import static org.elasticsearch.xpack.esql.session.SessionUtils.fromPages;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SessionUtilsTests extends ESTestCase {

    /*
     * 1. Generate a list of Pages with one BytesRef block, each of different positions, filled with random bytes.
     * 2. Convert the list of Pages into a single BytesRefBlock Page using `fromPages()`.
     * 3. Verify that the resulting BytesRefBlock contains the same bytes from the input Pages.
     * 4. Verify that a CircuitBreakingException is thrown when the memory limit is too low.
     */
    public void testFromPages() {
        final int minBytes = 500;
        final int maxBytes = randomIntBetween(minBytes, minBytes * 1_000);
        byte[] inBuffer = new byte[maxBytes];
        BlockFactory blockFactory = blockFactory((int) GB.toBytes(1));

        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(maxBytes);
        List<Page> pages = new ArrayList<>();
        int producedBytes = 0;
        int producedRows = 0;
        int rowsPerPage = randomIntBetween(1, 100);
        int rows = 0;
        while (producedBytes < maxBytes) {
            int rowBytes = Math.min(randomIntBetween(1, maxBytes / minBytes), maxBytes - producedBytes);
            byte[] rowValue = randomByteArrayOfLength(rowBytes);

            builder.appendBytesRef(new BytesRef(rowValue));
            System.arraycopy(rowValue, 0, inBuffer, producedBytes, rowBytes);

            producedBytes += rowBytes;
            rows++;

            if (rows > rowsPerPage) {
                producedRows += rows;
                rows = 0;
                enqueueBlock(builder, pages);
                builder = blockFactory.newBytesRefBlockBuilder(maxBytes);
                rowsPerPage = randomIntBetween(1, 100);
            }
        }
        if (rows > 0) {
            producedRows += rows;
            enqueueBlock(builder, pages);
        }

        Attribute attr = new ReferenceAttribute(randomSource(), randomAlphaOfLengthOrNull(10), randomAlphaOfLength(10), DataType.KEYWORD);

        Block[] outBlocks = fromPages(List.of(attr), pages, blockFactory);
        assertThat(outBlocks.length, is(1));
        BytesRefBlock bytesBlock = (BytesRefBlock) outBlocks[0];
        assertThat(bytesBlock.getPositionCount(), is(producedRows));

        byte[] outBuffer = new byte[producedBytes];
        for (int i = 0, posCount = bytesBlock.getPositionCount(), outOffset = 0; i < posCount; i++) {
            BytesRef ref = bytesBlock.getBytesRef(i, new BytesRef());
            System.arraycopy(ref.bytes, ref.offset, outBuffer, outOffset, ref.length);
            outOffset += ref.length;
        }
        assertThat(outBuffer, is(inBuffer));

        Releasables.close(outBlocks);

        BlockFactory convertBlockFactory = blockFactory(minBytes);
        assertThrows(CircuitBreakingException.class, () -> fromPages(List.of(attr), pages, convertBlockFactory));

        Releasables.close(pages);
    }

    private BlockFactory blockFactory(long maxBytes) {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(this.getClass().getName(), ByteSizeValue.ofBytes(maxBytes));
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
        return new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
    }

    private void enqueueBlock(BytesRefBlock.Builder builder, List<Page> pages) {
        Block block = builder.build();
        pages.add(new Page(block));
        Releasables.close(builder);
    }
}
