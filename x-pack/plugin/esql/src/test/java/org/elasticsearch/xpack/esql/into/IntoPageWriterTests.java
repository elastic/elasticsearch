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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
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
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IntoPageWriterTests extends ComputeTestCase {

    private static final String DESTINATION = "dst";
    private static final ShardId SHARD = new ShardId(DESTINATION, "uuid", 0);
    private static final List<Attribute> COLUMNS = List.of(
        new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD),
        new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME)
    );

    private ThreadPool threadPool;
    private CapturingClient client;
    private BlockFactory blockFactory;
    private DriverContext driverContext;
    private IntoPageWriter.Factory sink;
    private IntoPageWriter writer;

    @Before
    public void setUpWriter() {
        threadPool = new TestThreadPool(getTestName());
        client = new CapturingClient(threadPool);
        blockFactory = blockFactory();
        driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        sink = new IntoPageWriter.Factory(client, DESTINATION, COLUMNS);
        writer = sink.create(driverContext);
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
        final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();

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
            assertThat(action, equalTo(TransportBulkAction.TYPE));
            bulks.add((BulkRequest) request);
            listeners.add((ActionListener<BulkResponse>) listener);
        }

        void answer(Outcome... outcomes) {
            BulkRequest bulk = bulks.remove(0);
            assertThat(bulk.numberOfActions(), equalTo(outcomes.length));
            BulkItemResponse[] items = new BulkItemResponse[outcomes.length];
            for (int i = 0; i < outcomes.length; i++) {
                DocWriteRequest.OpType opType = bulk.requests().get(i).opType();
                items[i] = switch (outcomes[i]) {
                    case CREATED -> BulkItemResponse.success(i, opType, new IndexResponse(SHARD, "id-" + i, 0, 1, 1, true));
                    case REFUSED -> BulkItemResponse.failure(
                        i,
                        opType,
                        new BulkItemResponse.Failure(DESTINATION, "id-" + i, new IllegalArgumentException("mapper_parsing_exception"))
                    );
                };
            }
            listeners.remove(0).onResponse(new BulkResponse(items, 1));
        }

        void fail(Exception e) {
            bulks.remove(0);
            listeners.remove(0).onFailure(e);
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
        List<DocWriteRequest<?>> requests = client.bulks.get(0).requests();
        assertThat(requests.stream().map(DocWriteRequest::opType).toList(), everyItem(equalTo(DocWriteRequest.OpType.CREATE)));
        assertThat(requests.stream().map(DocWriteRequest::index).toList(), everyItem(equalTo(DESTINATION)));
        assertThat(
            requests.stream().map(request -> ((IndexRequest) request).sourceAsMap()).toList(),
            equalTo(
                List.of(Map.of("host", List.of("h1", "h2"), "ts", "2025-06-15T15:06:40.000Z"), Map.of("ts", "1970-01-01T00:00:00.000Z"))
            )
        );
    }

    public void testBackpressureAtMaxConcurrentBulks() {
        for (int i = 0; i < IntoPageWriter.MAX_CONCURRENT_BULKS; i++) {
            assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
            writer.consume(page("h" + i));
        }
        IsBlockedResult blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
    }

    public void testFinishWaitsForOutstandingBulks() {
        writer.consume(page("h1"));
        writer.consume(page("h2"));
        writer.finish();
        IsBlockedResult blocked = writer.isBlocked();

        client.answer(Outcome.CREATED);
        assertFalse(writer.isFinished());
        assertFalse(blocked.listener().isDone());
        assertTrue(driverContext.hasPendingAsyncActions());

        client.answer(Outcome.CREATED);
        assertTrue(writer.isFinished());
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(sink.failure(), nullValue());
        assertThat(sink.stats(), equalTo(new IntoStats(2, 2)));
    }

    public void testRefusedRowFailsAndLaterPagesAreDropped() {
        writer.consume(page("ok", "bad"));
        client.answer(Outcome.CREATED, Outcome.REFUSED);
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
        assertThat(sink.stats(), equalTo(new IntoStats(0, 0)));
    }

    private Page page(String... hosts) {
        try (BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(hosts.length)) {
            for (String value : hosts) {
                host.appendBytesRef(new BytesRef(value));
            }
            return new Page(host.build(), blockFactory.newConstantLongBlockWith(0L, hosts.length));
        }
    }
}
