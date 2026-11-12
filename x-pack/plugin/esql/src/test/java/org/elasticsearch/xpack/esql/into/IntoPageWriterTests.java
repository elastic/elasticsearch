/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IntoPageWriterTests extends ESTestCase {

    private static final String DESTINATION = "dst";
    private static final ShardId SHARD = new ShardId(DESTINATION, "uuid", 0);

    private static final List<Attribute> COLUMNS = List.of(
        new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD),
        new ReferenceAttribute(Source.EMPTY, "cpu", DataType.DOUBLE),
        new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME)
    );

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
    private ThreadPool threadPool;
    private CapturingClient client;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        client = new CapturingClient(threadPool);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private enum Outcome {
        CREATED,
        REFUSED
    }

    private static class CapturingClient extends NoOpClient {
        final List<BulkRequest> bulks = new ArrayList<>();
        final List<ActionListener<BulkResponse>> bulkListeners = new ArrayList<>();

        CapturingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, sameInstance(TransportBulkAction.TYPE));
            bulks.add((BulkRequest) request);
            bulkListeners.add((ActionListener<BulkResponse>) listener);
        }

        void answer(Outcome... outcomes) {
            BulkRequest bulk = bulks.remove(0);
            ActionListener<BulkResponse> listener = bulkListeners.remove(0);
            assertThat(outcomes.length, equalTo(bulk.numberOfActions()));
            BulkItemResponse[] items = new BulkItemResponse[outcomes.length];
            for (int i = 0; i < outcomes.length; i++) {
                DocWriteRequest<?> request = bulk.requests().get(i);
                String id = "auto-" + i;
                items[i] = switch (outcomes[i]) {
                    case CREATED -> BulkItemResponse.success(i, request.opType(), new IndexResponse(SHARD, id, 0, 1, 1, true));
                    case REFUSED -> BulkItemResponse.failure(
                        i,
                        request.opType(),
                        new BulkItemResponse.Failure(DESTINATION, id, new IllegalArgumentException("mapper_parsing_exception: bad value"))
                    );
                };
            }
            listener.onResponse(new BulkResponse(items, 1));
        }
    }

    public void testPageToSourceConversion() {
        IntoPageWriter writer = new IntoPageWriter(client, DESTINATION, COLUMNS);
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(2);
            DoubleBlock.Builder cpu = blockFactory.newDoubleBlockBuilder(2);
            LongBlock.Builder ts = blockFactory.newLongBlockBuilder(2)
        ) {
            host.beginPositionEntry();
            host.appendBytesRef(new BytesRef("h1"));
            host.appendBytesRef(new BytesRef("h2"));
            host.endPositionEntry();
            cpu.appendDouble(1.5);
            ts.appendLong(1_750_000_000_000L);

            host.appendNull();
            cpu.appendNull();
            ts.appendLong(0L);

            writer.accept(new Page(host.build(), cpu.build(), ts.build()));
        }

        assertThat(client.bulks, hasSize(1));
        BulkRequest bulk = client.bulks.get(0);
        assertThat(bulk.requests(), hasSize(2));

        IndexRequest first = (IndexRequest) bulk.requests().get(0);
        assertThat(first.index(), equalTo(DESTINATION));
        assertThat(first.opType(), equalTo(DocWriteRequest.OpType.CREATE));
        assertThat(first.sourceAsMap(), equalTo(Map.of("host", List.of("h1", "h2"), "cpu", 1.5, "ts", "2025-06-15T15:06:40.000Z")));

        IndexRequest second = (IndexRequest) bulk.requests().get(1);
        assertThat(second.opType(), equalTo(DocWriteRequest.OpType.CREATE));
        assertThat(second.sourceAsMap(), equalTo(Map.of("ts", "1970-01-01T00:00:00.000Z")));
    }

    public void testBackpressureAtMaxConcurrentBulks() {
        IntoPageWriter writer = new IntoPageWriter(client, DESTINATION, COLUMNS);
        for (int i = 0; i < IntoPageWriter.MAX_CONCURRENT_BULKS - 1; i++) {
            writer.accept(singleRowPage("h-" + i));
            assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        }

        writer.accept(singleRowPage("h-max"));
        assertThat(client.bulks, hasSize(IntoPageWriter.MAX_CONCURRENT_BULKS));
        IsBlockedResult blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
    }

    public void testIsFinishedWaitsForOutstandingBulks() {
        IntoPageWriter writer = new IntoPageWriter(client, DESTINATION, COLUMNS);
        writer.accept(singleRowPage("h1"));
        writer.accept(singleRowPage("h2"));

        writer.finish();
        assertFalse(writer.isFinished());
        IsBlockedResult blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertFalse(writer.isFinished());
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertTrue(writer.isFinished());
        assertTrue(blocked.listener().isDone());
        assertThat(writer.failure(), nullValue());
        assertThat(writer.stats().total(), equalTo(2L));
        assertThat(writer.stats().created(), equalTo(2L));
    }

    public void testRefusedRowSetsFailureAndSkipsLaterPages() {
        IntoPageWriter writer = new IntoPageWriter(client, DESTINATION, COLUMNS);
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(2);
            DoubleBlock.Builder cpu = blockFactory.newDoubleBlockBuilder(2);
            LongBlock.Builder ts = blockFactory.newLongBlockBuilder(2)
        ) {
            host.appendBytesRef(new BytesRef("ok"));
            cpu.appendDouble(1.0);
            ts.appendLong(0L);
            host.appendBytesRef(new BytesRef("bad"));
            cpu.appendDouble(2.0);
            ts.appendLong(0L);
            writer.accept(new Page(host.build(), cpu.build(), ts.build()));
        }

        client.answer(Outcome.CREATED, Outcome.REFUSED);
        assertThat(writer.failure(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException statusException = (ElasticsearchStatusException) writer.failure();
        assertThat(statusException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(statusException.getMessage(), containsString("mapper_parsing_exception"));
        assertThat(writer.stats().created(), equalTo(1L));

        writer.accept(singleRowPage("skipped"));
        assertThat(client.bulks, empty());
    }

    public void testEmptyPageSendsNothing() {
        IntoPageWriter writer = new IntoPageWriter(client, DESTINATION, COLUMNS);
        writer.accept(
            new Page(blockFactory.newConstantNullBlock(0), blockFactory.newConstantNullBlock(0), blockFactory.newConstantNullBlock(0))
        );
        writer.finish();

        assertThat(client.bulks, empty());
        assertTrue(writer.isFinished());
        assertThat(writer.stats().total(), equalTo(0L));
        assertThat(writer.stats().created(), equalTo(0L));
    }

    private Page singleRowPage(String hostValue) {
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(1);
            DoubleBlock.Builder cpu = blockFactory.newDoubleBlockBuilder(1);
            LongBlock.Builder ts = blockFactory.newLongBlockBuilder(1)
        ) {
            host.appendBytesRef(new BytesRef(hostValue));
            cpu.appendDouble(1.0);
            ts.appendLong(0L);
            return new Page(host.build(), cpu.build(), ts.build());
        }
    }
}
