/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class FetchResponseTests extends ComputeTestCase {
    private static final List<ElementType> TYPES = List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.BOOLEAN);

    public void testRoundTrip() throws IOException {
        BlockFactory factory = blockFactory();
        List<Page> pages = randomPages(factory);
        int rows = pages.stream().mapToInt(Page::getPositionCount).sum();
        DriverCompletionInfo info = new DriverCompletionInfo(
            3,
            5,
            rows,
            11,
            13,
            17,
            19,
            31,
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );

        // the original keeps its pages until it is closed, so the copy compares against them
        try (FetchResponse response = new FetchResponse(pages, info, 23, 29); FetchResponse copy = copy(factory, response)) {
            assertThat(copy.rows(), equalTo(rows));
            assertThat(copy.completionInfo(), equalTo(info));
            assertThat(copy.tookNanos(), equalTo(23L));
            assertThat(copy.setupNanos(), equalTo(29L));
            List<Page> taken = copy.takePages();
            try {
                assertThat(taken, equalTo(pages));
            } finally {
                Releasables.closeExpectNoException(taken);
            }
        }
    }

    /**
     * The channel that sends the response charges the bytes it writes until they are sent, so writing reserves nothing of
     * its own.
     */
    public void testSerializingReservesNothing() throws IOException {
        BlockFactory factory = blockFactory();
        FetchResponse response = new FetchResponse(randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);
        long blocks = factory.breaker().getUsed();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
        }
        assertThat(factory.breaker().getUsed(), equalTo(blocks));

        response.close();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The node that reads a response accounts its pages on its own breaker until they are released.
     */
    public void testTheReceiverAccountsThePagesItReads() throws IOException {
        BlockFactory sender = blockFactory();
        BlockFactory receiver = blockFactory();
        try (
            FetchResponse response = new FetchResponse(randomPages(sender), DriverCompletionInfo.EMPTY, 0, 0);
            FetchResponse copy = copy(receiver, response)
        ) {
            List<Page> taken = copy.takePages();
            long bytes = taken.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
            assertThat(receiver.breaker().getUsed(), equalTo(bytes));
            Releasables.closeExpectNoException(taken);
        }
        assertThat(receiver.breaker().getUsed(), equalTo(0L));
    }

    /**
     * A response too large for the breaker of the node that reads it fails to read, and leaves nothing on that breaker.
     * That is what bounds a response, since one request carries every document a shard holds for the cut.
     */
    public void testAResponseTooLargeForTheReceiverLeavesNothingOnItsBreaker() throws IOException {
        BlockFactory sender = blockFactory();
        BlockFactory receiver = blockFactory(ByteSizeValue.ofBytes(between(1, 100)));
        try (FetchResponse response = new FetchResponse(randomPages(sender), DriverCompletionInfo.EMPTY, 0, 0)) {
            expectThrows(CircuitBreakingException.class, () -> copy(receiver, response));
        }
        assertThat(receiver.breaker().getUsed(), equalTo(0L));
    }

    public void testReleasesThePagesNobodyTook() {
        BlockFactory factory = blockFactory();
        FetchResponse response = new FetchResponse(randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);

        response.close();

        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testHandsThePagesOverOnce() {
        BlockFactory factory = blockFactory();
        try (FetchResponse response = new FetchResponse(randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0)) {
            Releasables.closeExpectNoException(response.takePages());
            expectThrows(AssertionError.class, response::takePages);
        }
    }

    /**
     * A response cut short after its pages releases the pages it already read.
     */
    public void testReleasesThePagesOfATruncatedResponse() throws IOException {
        BlockFactory factory = blockFactory();
        BytesReference bytes;
        try (
            FetchResponse response = new FetchResponse(randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);
            BytesStreamOutput out = new BytesStreamOutput()
        ) {
            response.writeTo(out);
            bytes = out.copyBytes();
        }
        // the last byte is the setup time, read after every page
        BytesReference truncated = bytes.slice(0, bytes.length() - 1);
        try (BlockStreamInput in = new BlockStreamInput(truncated.streamInput(), factory)) {
            expectThrows(EOFException.class, () -> new FetchResponse(in, new ThreadContext(Settings.EMPTY)));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Every version with the fetch phase reads a response the same way. A node only answers a fetch request, which only
     * those versions send.
     */
    public void testRoundTripsAtEveryVersionWithTheFetchPhase() throws IOException {
        BlockFactory factory = blockFactory();
        List<Page> pages = randomPages(factory);
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(FetchRequest.ESQL_FETCH);
        try (
            FetchResponse response = new FetchResponse(pages, DriverCompletionInfo.EMPTY, 23, 29);
            FetchResponse copy = copy(factory, response, version)
        ) {
            assertThat(copy.tookNanos(), equalTo(23L));
            List<Page> taken = copy.takePages();
            try {
                assertThat(taken, equalTo(pages));
            } finally {
                Releasables.closeExpectNoException(taken);
            }
        }
    }

    private static List<Page> randomPages(BlockFactory factory) {
        int count = between(1, 4);
        List<Page> pages = new ArrayList<>(count);
        for (int p = 0; p < count; p++) {
            int positions = between(1, 20);
            Block[] blocks = new Block[TYPES.size()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = RandomBlock.randomBlock(factory, TYPES.get(b), positions, randomBoolean(), 1, 3, 0, 0).block();
            }
            pages.add(new Page(blocks));
        }
        return pages;
    }

    private static FetchResponse copy(BlockFactory factory, FetchResponse response) throws IOException {
        return copy(factory, response, TransportVersion.current());
    }

    private static FetchResponse copy(BlockFactory factory, FetchResponse response, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            response.writeTo(out);
            try (BlockStreamInput in = new BlockStreamInput(out.bytes().streamInput(), factory)) {
                in.setTransportVersion(version);
                return new FetchResponse(in, new ThreadContext(Settings.EMPTY));
            }
        }
    }
}
