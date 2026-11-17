/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.Outcome.CREATED;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.Outcome.REFUSED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IntoPageWriterTests extends ComputeTestCase {
    private static final List<Attribute> COLUMNS = List.of(
        new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD),
        new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME)
    );

    private ThreadPool threadPool;
    private IntoTestUtils.CapturingClient client;
    private BlockFactory blockFactory;
    private DriverContext driverContext;
    private IntoPageWriter.Factory sink;
    private IntoPageWriter writer;

    @Before
    public void setUpWriter() {
        threadPool = new TestThreadPool(getTestName());
        client = new IntoTestUtils.CapturingClient(threadPool);
        blockFactory = blockFactory();
        driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        sink = IntoPageWriter.Factory.createFactory(client, IntoTestUtils.DESTINATION, COLUMNS, INDEX);
        writer = sink.create(driverContext);
    }

    @After
    public void stopThreadPool() {
        if (client != null) {
            while (client.listeners.isEmpty() == false) {
                client.fail(new EsRejectedExecutionException("test cleanup"));
            }
        }
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testPageToSourceConversion() {
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(2);
            LongBlock.Builder ts = blockFactory.newLongBlockBuilder(2)
        ) {
            host.beginPositionEntry().appendBytesRef(new BytesRef("h1")).appendBytesRef(new BytesRef("h2")).endPositionEntry();
            ts.appendLong(1_750_000_000_000L);
            host.appendNull();
            ts.appendLong(0L);
            writer.consume(new Page(host.build(), ts.build()));
        }

        assertThat(client.bulks, hasSize(1));
        List<DocWriteRequest<?>> requests = client.bulks.getFirst().requests();
        assertThat(requests.stream().map(DocWriteRequest::opType).toList(), everyItem(equalTo(DocWriteRequest.OpType.CREATE)));
        assertThat(requests.stream().map(DocWriteRequest::index).toList(), everyItem(equalTo(IntoTestUtils.DESTINATION)));
        assertThat(
            requests.stream().map(request -> ((IndexRequest) request).sourceAsMap()).toList(),
            equalTo(
                List.of(Map.of("host", List.of("h1", "h2"), "ts", "2025-06-15T15:06:40.000Z"), Map.of("ts", "1970-01-01T00:00:00.000Z"))
            )
        );
    }

    public void testBackpressureAtInFlightByteLimit() {
        for (int i = 0; i < 3; i++) {
            assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
            writer.consume(widePage());
        }
        assertThat(driverContext.breaker().getUsed(), greaterThanOrEqualTo(IntoPageWriter.MAX_IN_FLIGHT_BYTES));
        IsBlockedResult blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(CREATED);
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
        assertThat(driverContext.breaker().getUsed(), greaterThan(0L));

        client.answer(CREATED);
        client.answer(CREATED);
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testSourceBytesChargedUntilBulkSettles() {
        writer.consume(page("h1", "h2"));
        long sourceBytes = client.bulks.getFirst()
            .requests()
            .stream()
            .mapToLong(request -> ((IndexRequest) request).indexSource().byteLength())
            .sum();

        assertThat(sourceBytes, greaterThan(0L));
        assertThat(driverContext.breaker().getUsed(), equalTo(sourceBytes));

        client.answer(CREATED, CREATED);
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testCircuitBreakerTripReleasesReservedRows() {
        writer.consume(page("h1"));
        long oneRow = driverContext.breaker().getUsed();
        client.answer(CREATED);
        assertThat(oneRow, greaterThan(0L));

        BlockFactory limited = blockFactory(ByteSizeValue.ofBytes(oneRow));
        IntoPageWriter limitedWriter = sink.create(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, limited, null));
        expectThrows(CircuitBreakingException.class, () -> limitedWriter.consume(page("h1", "h2")));
        assertThat(client.bulks, empty());
        assertThat(limited.breaker().getUsed(), equalTo(0L));
    }

    public void testLocalCircuitBreakersShareParentRequestBreakerLimit() {
        writer.consume(page("h1"));
        long oneRow = driverContext.breaker().getUsed();
        client.answer(CREATED);
        assertThat(oneRow, greaterThan(0L));

        BlockFactory shared = blockFactory(ByteSizeValue.ofBytes(oneRow * 2));
        LocalCircuitBreaker local1 = new LocalCircuitBreaker(shared.breaker(), 1, 2);
        LocalCircuitBreaker local2 = new LocalCircuitBreaker(shared.breaker(), 1, 2);
        try {
            IntoPageWriter first = sink.create(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, shared.newChildFactory(local1), null));
            IntoPageWriter second = sink.create(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, shared.newChildFactory(local2), null));
            first.consume(page("h1"));
            local2.addEstimateBytesAndMaybeBreak(1, "driver");

            expectThrows(CircuitBreakingException.class, () -> second.consume(page("h2")));
            assertThat(client.bulks, hasSize(1));
        } finally {
            client.answer(CREATED);
            local2.addWithoutBreaking(-1);
            local1.close();
            local2.close();
        }
        assertThat(shared.breaker().getUsed(), equalTo(0L));
    }

    public void testCrankyBreakerReleasesAndStatsMatchWrittenRows() {
        BlockFactory crankyFactory = crankyBlockFactory();
        IntoPageWriter crankyWriter = sink.create(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, crankyFactory, null));
        int written = 0;
        boolean tripped = false;
        for (int i = 0; i < 1000; i++) {
            try {
                crankyWriter.consume(page("h" + i));
            } catch (CircuitBreakingException e) {
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                tripped = true;
                break;
            }
            int actions = client.bulks.getFirst().numberOfActions();
            client.answer(CREATED);
            written += actions;
        }
        assertTrue(tripped);
        assertThat(client.bulks, empty());
        assertThat(crankyFactory.breaker().getUsed(), equalTo(0L));
        assertThat(crankyWriter.stats(), equalTo(new IntoStats(written, written)));
    }

    public void testFinishWaitsForOutstandingBulks() {
        writer.consume(page("h1"));
        writer.consume(page("h2"));
        writer.finish();
        IsBlockedResult blocked = writer.isBlocked();

        client.answer(CREATED);
        assertFalse(writer.isFinished());
        assertFalse(blocked.listener().isDone());
        assertTrue(driverContext.hasPendingAsyncActions());

        client.answer(CREATED);
        assertTrue(writer.isFinished());
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(sink.failure(), nullValue());
        assertThat(writer.stats(), equalTo(new IntoStats(2, 2)));
    }

    public void testRefusedRowFailsAndLaterPagesAreDropped() {
        writer.consume(page("ok", "bad"));
        client.answer(CREATED, REFUSED);
        ElasticsearchStatusException failure = asInstanceOf(ElasticsearchStatusException.class, sink.failure());
        assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.getMessage(), containsString("mapper_parsing_exception"));

        writer.consume(page("dropped"));
        assertThat(client.bulks, empty());
    }

    public void testWholeBulkFailureIsRecordedAndSettles() {
        writer.consume(page("h1"));
        Exception rejected = new EsRejectedExecutionException("rejected");
        client.fail(rejected);
        writer.finish();

        assertTrue(writer.isFinished());
        assertThat(sink.failure(), sameInstance(rejected));
        assertFalse(driverContext.hasPendingAsyncActions());
    }

    public void testEmptyPageSendsNothing() {
        writer.consume(page());
        writer.finish();

        assertThat(client.bulks, empty());
        assertTrue(writer.isFinished());
        assertThat(writer.stats(), equalTo(new IntoStats(0, 0)));
    }

    private Page page(String... hosts) {
        try (BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(hosts.length)) {
            for (String value : hosts) {
                host.appendBytesRef(new BytesRef(value));
            }
            return new Page(host.build(), blockFactory.newConstantLongBlockWith(0L, hosts.length));
        }
    }

    private Page widePage() {
        byte[] value = new byte[Operator.TARGET_PAGE_SIZE];
        Arrays.fill(value, (byte) 'a');
        return page(new String(value, StandardCharsets.UTF_8));
    }
}
