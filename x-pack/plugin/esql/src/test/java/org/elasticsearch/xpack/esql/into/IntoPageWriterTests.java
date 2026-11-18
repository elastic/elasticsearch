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
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        driverContext = new DriverContext(mockBigArrays(), blockFactory, null);
        sink = IntoPageWriter.Factory.createFactory(bulks(), client, blockFactory.breaker(), DESTINATION, COLUMNS);
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
            BulkRequest bulk = bulks.removeFirst();
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
            listeners.removeFirst().onResponse(new BulkResponse(items, 1));
        }

        void fail(Exception e) {
            bulks.removeFirst();
            listeners.removeFirst().onFailure(e);
        }
    }

    private static List<Map<String, Object>> sources(BulkRequest bulk) throws IOException {
        List<Map<String, Object>> sources = new ArrayList<>();
        for (DocWriteRequest<?> request : bulk.requests()) {
            IndexRequest index = (IndexRequest) request;
            index.indexSource().ensureInlineSource();
            sources.add(XContentHelper.convertToMap(index.indexSource().bytes(), false, XContentType.JSON).v2());
        }
        return sources;
    }

    public void testPageToSourceConversion() throws IOException {
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
        assertThat(requests.stream().map(DocWriteRequest::index).toList(), everyItem(equalTo(DESTINATION)));
        assertThat(
            sources(client.bulks.getFirst()),
            equalTo(
                List.of(Map.of("host", List.of("h1", "h2"), "ts", "2025-06-15T15:06:40.000Z"), Map.of("ts", "1970-01-01T00:00:00.000Z"))
            )
        );
    }

    public void testEscfBatchProperties() throws IOException {
        IntoPageWriter.Factory escfSink = IntoPageWriter.Factory.createFactory(
            bulks(),
            client,
            blockFactory.breaker(),
            DESTINATION,
            COLUMNS
        );
        IntoPageWriter escfWriter = escfSink.create(new DriverContext(mockBigArrays(), blockFactory, null));
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(2);
            LongBlock.Builder ts = blockFactory.newLongBlockBuilder(2)
        ) {
            host.beginPositionEntry().appendBytesRef(new BytesRef("h1")).appendBytesRef(new BytesRef("h2")).endPositionEntry();
            ts.appendLong(1_750_000_000_000L);
            host.appendNull();
            ts.appendLong(0L);
            escfWriter.consume(new Page(host.build(), ts.build()));
        }

        assertThat(client.bulks, hasSize(1));
        BulkRequest bulk = client.bulks.getFirst();
        assertTrue(bulk.hasPreBuiltBatches());
        assertThat(bulk.requests().stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList(), equalTo(List.of(0, 1)));
        assertThat(
            sources(bulk),
            equalTo(
                List.of(Map.of("host", List.of("h1", "h2"), "ts", "2025-06-15T15:06:40.000Z"), Map.of("ts", "1970-01-01T00:00:00.000Z"))
            )
        );
        client.answer(Outcome.CREATED, Outcome.CREATED);
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testEscfBatchBytesIncludeRecyclerOverhead() {
        BlockFactory limited = blockFactory();
        DriverContext escfContext = new DriverContext(mockBigArrays(), limited, null);
        IntoPageWriter.Factory escfSink = IntoPageWriter.Factory.createFactory(bulks(), client, limited.breaker(), DESTINATION, COLUMNS);
        IntoPageWriter escfWriter = escfSink.create(escfContext);
        escfWriter.consume(page("h1"));
        long charged = escfContext.breaker().getUsed();
        long twoPageMin = 2L * escfContext.bigArrays().bytesRefRecycler().pageSize();
        assertThat(charged, greaterThanOrEqualTo(twoPageMin));
        client.answer(Outcome.CREATED);
        assertThat(escfContext.breaker().getUsed(), equalTo(0L));
    }

    public void testBreakerTripReleasesTheCharge() {
        // One row charges more than a page. The trip must release that charge and send nothing.
        long pageSize = driverContext.bigArrays().bytesRefRecycler().pageSize();
        BlockFactory limited = blockFactory(ByteSizeValue.ofBytes(pageSize + 100));
        IntoPageWriter.Factory escfSink = IntoPageWriter.Factory.createFactory(bulks(), client, limited.breaker(), DESTINATION, COLUMNS);
        IntoPageWriter escfWriter = escfSink.create(new DriverContext(mockBigArrays(), limited, null));
        expectThrows(CircuitBreakingException.class, () -> escfWriter.consume(page("h1")));
        assertThat(limited.breaker().getUsed(), equalTo(0L));
        assertThat(client.bulks, empty());
    }

    public void testBackpressureAtInFlightByteLimit() {
        int pages = IntoPageWriter.POLICY.maxBulksInFlight() + 1;
        for (int i = 0; i < pages; i++) {
            assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
            writer.consume(widePage());
        }
        assertThat(driverContext.breaker().getUsed(), greaterThan(0L));
        IsBlockedResult blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));

        while (client.bulks.isEmpty() == false) {
            client.answer(Outcome.CREATED);
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testSourceBytesChargedUntilBulkSettles() {
        writer.consume(page("h1", "h2"));
        long charged = driverContext.breaker().getUsed();
        assertThat(charged, greaterThan(0L));

        client.answer(Outcome.CREATED, Outcome.CREATED);
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testCircuitBreakerTripReleasesReservedRows() {
        writer.consume(page("h1"));
        long oneRow = driverContext.breaker().getUsed();
        client.answer(Outcome.CREATED);
        assertThat(oneRow, greaterThan(0L));

        BlockFactory limited = blockFactory(ByteSizeValue.ofBytes(oneRow));
        IntoPageWriter limitedWriter = IntoPageWriter.Factory.createFactory(bulks(), client, limited.breaker(), DESTINATION, COLUMNS)
            .create(new DriverContext(mockBigArrays(), limited, null));
        expectThrows(CircuitBreakingException.class, () -> limitedWriter.consume(page("h1", "h2")));
        assertThat(client.bulks, empty());
        assertThat(limited.breaker().getUsed(), equalTo(0L));
    }

    public void testCrankyBreakerReleasesAndStatsMatchWrittenRows() {
        BlockFactory crankyFactory = crankyBlockFactory();
        IntoPageWriter.Factory crankySink = IntoPageWriter.Factory.createFactory(
            bulks(),
            client,
            crankyFactory.breaker(),
            DESTINATION,
            COLUMNS
        );
        IntoPageWriter crankyWriter = crankySink.create(new DriverContext(mockBigArrays(), crankyFactory, null));
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
            client.answer(Outcome.CREATED);
            written += actions;
        }
        assertTrue(tripped);
        assertThat(client.bulks, empty());
        assertThat(crankyFactory.breaker().getUsed(), equalTo(0L));
        assertThat(crankySink.stats(), equalTo(new IntoStats(written, written)));
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

    public void testCloseAfterFinishKeepsTheAsyncActionUntilBulksSettle() {
        writer.consume(page("h1"));
        writer.finish();
        writer.close();
        assertTrue(driverContext.hasPendingAsyncActions());
        client.answer(Outcome.CREATED);
        assertFalse(driverContext.hasPendingAsyncActions());
    }

    public void testCloseAfterFailureReleasesTheAsyncAction() {
        Attribute column = new ReferenceAttribute(Source.EMPTY, "n", DataType.UNSIGNED_LONG);
        IntoPageWriter exactWriter = IntoPageWriter.Factory.createFactory(
            bulks(),
            client,
            driverContext.breaker(),
            DESTINATION,
            List.of(column)
        ).create(driverContext);
        expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (LongBlock.Builder values = blockFactory.newLongBlockBuilder(1)) {
                values.appendLong(NumericUtils.asLongUnsigned(BigInteger.TWO.pow(63).add(BigInteger.ONE)));
                exactWriter.consume(new Page(values.build()));
            }
        });
        assertTrue(driverContext.hasPendingAsyncActions());
        exactWriter.close();
        assertFalse(driverContext.hasPendingAsyncActions());
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

    public void testUnsignedLongAboveLongMaxIsRejected() {
        BigInteger big = BigInteger.TWO.pow(63).add(BigInteger.ONE);
        Attribute column = new ReferenceAttribute(Source.EMPTY, "n", DataType.UNSIGNED_LONG);
        IntoPageWriter exactWriter = IntoPageWriter.Factory.createFactory(
            bulks(),
            client,
            driverContext.breaker(),
            DESTINATION,
            List.of(column)
        ).create(driverContext);
        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (LongBlock.Builder values = blockFactory.newLongBlockBuilder(1)) {
                values.appendLong(NumericUtils.asLongUnsigned(big));
                exactWriter.consume(new Page(values.build()));
            }
        });
        assertThat(e.getMessage(), containsString("unsigned_long"));
    }

    public void testPipelineDestinationIsRejected() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "my-pipeline"))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> IntoPageWriter.Factory.requireBatchDestination(true, project, "dest")
        );
        assertThat(e.getMessage(), containsString("ingest pipelines"));
    }

    public void testBatchIndexingDisabledIsRejected() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> IntoPageWriter.Factory.requireBatchDestination(false, project, "dest")
        );
        assertThat(e.getMessage(), containsString("requires batch indexing"));
    }

    public void testConcreteIndexWithoutRoutingPathIsEligible() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("dest").settings(indexSettings(IndexVersion.current(), 1, 0)))
            .build();
        assertPreBuilt(project, true);
    }

    public void testIndexWithDefaultPipelineIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "my-pipeline"))
            .build();
        assertPreBuilt(ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build(), false);
    }

    public void testIndexWithFinalPipelineIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.FINAL_PIPELINE.getKey(), "my-final-pipeline"))
            .build();
        assertPreBuilt(ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build(), false);
    }

    public void testConcreteIndexWithRoutingPathIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> IntoPageWriter.Factory.requireBatchDestination(true, project, "dest")
        );
        assertThat(e.getMessage(), containsString("route from _source"));
    }

    // Before the E6 fix, project.index("my-alias") returned null so routesFromSource returned false.
    public void testAliasWhoseWriteIndexHasRoutingPathIsNotEligible() {
        IndexMetadata backing = IndexMetadata.builder("backing-000001")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .putAlias(AliasMetadata.builder("my-alias").writeIndex(true))
            .build();
        assertPreBuilt(ProjectMetadata.builder(randomProjectIdOrDefault()).put(backing, true).build(), "my-alias", false);
    }

    public void testDataStreamWhoseWriteIndexHasRoutingPathIsNotEligible() {
        IndexMetadata backing = IndexMetadata.builder(".ds-metrics-000001")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .build();
        Index backingIndex = backing.getIndex();
        DataStream ds = DataStream.builder("metrics", List.of(backingIndex)).build();
        assertPreBuilt(ProjectMetadata.builder(randomProjectIdOrDefault()).put(backing, true).put(ds).build(), "metrics", false);
    }

    public void testMissingNameMatchingV2TemplateIsNotEligible() {
        ComposableIndexTemplate template = ComposableIndexTemplate.builder().indexPatterns(List.of("metrics-*")).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put("my-template", template).build();
        assertPreBuilt(project, "metrics-logs-123", false);
    }

    public void testMissingIndexWithNoTemplateIsEligible() {
        assertPreBuilt(ProjectMetadata.builder(randomProjectIdOrDefault()).build(), "new-dest", true);
    }

    public void testLegacyTemplateIsNotEligible() {
        IndexTemplateMetadata template = IndexTemplateMetadata.builder("legacy")
            .patterns(List.of("legacy-*"))
            .settings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host").build())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(template).build();
        assertPreBuilt(project, "legacy-logs", false);
    }

    public void testWritersShareCapacity() {
        IntoPageWriter writerB = sink.create(driverContext);

        for (int i = 0; i < IntoPageWriter.POLICY.maxBulksInFlight() + 1; i++) {
            writer.consume(widePage());
        }
        assertFalse(writer.isBlocked().listener().isDone());
        assertFalse(writerB.isBlocked().listener().isDone());

        client.answer(Outcome.CREATED);
        assertThat(writer.isBlocked(), equalTo(Operator.NOT_BLOCKED));
        assertThat(writerB.isBlocked(), equalTo(Operator.NOT_BLOCKED));
        client.answer(Outcome.CREATED);
        client.answer(Outcome.CREATED);
        client.answer(Outcome.CREATED);
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    private static void assertPreBuilt(ProjectMetadata project, boolean eligible) {
        assertPreBuilt(project, "dest", eligible);
    }

    private static void assertPreBuilt(ProjectMetadata project, String target, boolean eligible) {
        if (eligible) {
            IntoPageWriter.Factory.requireBatchDestination(true, project, target);
        } else {
            expectThrows(ElasticsearchStatusException.class, () -> IntoPageWriter.Factory.requireBatchDestination(true, project, target));
        }
    }

    private EscfBulkService bulks() {
        // Runs callbacks on the calling thread, so answer() settles the writer before it returns.
        IndexingPressure pressure = new IndexingPressure(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "1gb").build()
        );
        return new EscfBulkService(pressure, Runnable::run, mockBigArrays().bytesRefRecycler());
    }

    private static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), ByteSizeValue.ofGb(1));
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
