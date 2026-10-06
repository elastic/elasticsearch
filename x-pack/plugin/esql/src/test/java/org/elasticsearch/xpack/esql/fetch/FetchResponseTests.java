/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.xpack.esql.fetch.FetchResponse.ShardResult;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class FetchResponseTests extends ComputeTestCase {
    private static final List<ElementType> TYPES = List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.BOOLEAN);

    public void testRoundTrip() throws IOException {
        BlockFactory factory = blockFactory();
        List<Page> pages = randomPages(factory);
        int rows = pages.stream().mapToInt(Page::getPositionCount).sum();
        ShardSearchContextId missing = new ShardSearchContextId("session", 7L);
        List<ShardResult> results = List.of(
            ShardResult.succeeded(new ShardId("index", "uuid", 0), rows),
            ShardResult.failed(new ShardId("index", "uuid", 1), new SearchContextMissingException(missing)),
            ShardResult.failed(new ShardId("index", "uuid", 2), new ElasticsearchSecurityException("no", RestStatus.FORBIDDEN))
        );
        DriverCompletionInfo info = new DriverCompletionInfo(
            3,
            5,
            rows,
            11,
            13,
            17,
            19,
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );

        // the original keeps its pages until it is closed, so the copy compares against them
        try (
            FetchResponse response = new FetchResponse(factory, results, pages, info, 23, 29);
            FetchResponse copy = copy(factory, response)
        ) {
            assertThat(copy.shardResults().get(0), equalTo(results.get(0)));
            assertThat(copy.shardResults().get(1).failure(), instanceOf(SearchContextMissingException.class));
            assertThat(copy.shardResults().get(2).failure(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(copy.shardResults().get(2).rows(), equalTo(0));
            assertThat(copy.rows(), equalTo(rows));
            assertThat(copy.completionInfo(), equalTo(info));
            assertThat(copy.tookNanos(), equalTo(23L));
            assertThat(copy.setupNanos(), equalTo(29L));
            List<Page> taken = copy.takePages();
            try {
                assertThat(taken, equalTo(pages));
            } finally {
                FetchResponse.releasePages(taken);
            }
        }
    }

    /**
     * Serializing holds the pages twice, once as blocks and once as bytes, so the sender accounts them until the response
     * is released.
     */
    public void testAccountsThePagesWhileSerializing() throws IOException {
        BlockFactory factory = blockFactory();
        FetchResponse response = new FetchResponse(factory, List.of(), randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);
        long blocks = factory.breaker().getUsed();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
        }
        assertThat(factory.breaker().getUsed(), equalTo(blocks * 2));

        response.close();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testReleasesThePagesNobodyTook() {
        BlockFactory factory = blockFactory();
        FetchResponse response = new FetchResponse(factory, List.of(), randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);

        response.close();

        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testHandsThePagesOverOnce() {
        BlockFactory factory = blockFactory();
        try (FetchResponse response = new FetchResponse(factory, List.of(), randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0)) {
            FetchResponse.releasePages(response.takePages());
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
            FetchResponse response = new FetchResponse(factory, List.of(), randomPages(factory), DriverCompletionInfo.EMPTY, 0, 0);
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

    public void testFailedShardsHaveNoRows() {
        ShardResult failed = ShardResult.failed(new ShardId("index", "uuid", 0), new IllegalStateException("boom"));
        assertThat(failed.rows(), equalTo(0));
        assertThat(ShardResult.succeeded(new ShardId("index", "uuid", 0), 3).failure(), nullValue());
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
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (BlockStreamInput in = new BlockStreamInput(out.bytes().streamInput(), factory)) {
                return new FetchResponse(in, new ThreadContext(Settings.EMPTY));
            }
        }
    }
}
