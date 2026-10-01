/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;

public class EsqlStreamResponseListenerTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void newBlockFactory() {
        blockFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking()
        ).build();
    }

    @After
    public void blockFactoryEmpty() {
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    @SuppressWarnings("unchecked")
    public void testSinglePage() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);

        List<Map<String, Object>> lines = drainStream(s, List.of(buildSimplePage(42, "alice")), 100L, List.of());
        assertNotNull(s.response());
        assertThat(s.response().status(), equalTo(RestStatus.OK));
        assertThat(s.response().contentType(), equalTo("application/x-ndjson"));
        assertThat(lines.size(), equalTo(3));

        List<Map<String, Object>> cols = (List<Map<String, Object>>) lines.get(0).get("columns");
        assertThat(cols.size(), equalTo(2));
        assertThat(cols.get(0), equalTo(Map.of("name", "id", "type", "integer")));
        assertThat(cols.get(1), equalTo(Map.of("name", "name", "type", "keyword")));

        List<List<Object>> values = (List<List<Object>>) lines.get(1).get("values");
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0), equalTo(List.of(42, "alice")));

        assertThat(lines.get(2), equalTo(Map.of("status", 200, "took", 100, "is_partial", false, "warnings", List.of())));
    }

    @SuppressWarnings("unchecked")
    public void testMultiplePages() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Page> pages = List.of(buildSimplePage(1, "a"), buildSimplePage(2, "b"), buildSimplePage(3, "c"));
        List<Map<String, Object>> lines = drainStream(s, pages, 50L, List.of());
        assertThat(lines.size(), equalTo(5));

        for (int i = 0; i < 3; i++) {
            List<List<Object>> values = (List<List<Object>>) lines.get(i + 1).get("values");
            assertThat(values.size(), equalTo(1));
            assertThat(values.get(0), equalTo(List.of(i + 1, String.valueOf((char) ('a' + i)))));
        }
    }

    @SuppressWarnings("unchecked")
    public void testFooterWithWarnings() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Map<String, Object>> lines = drainStream(s, List.of(), 42L, List.of("warning1", "warning2"));
        assertThat(lines.size(), equalTo(2));

        Map<String, Object> footer = lines.get(1);
        assertThat(footer, equalTo(Map.of("status", 200, "took", 42, "is_partial", false, "warnings", List.of("warning1", "warning2"))));
    }

    public void testFooterIsPartial() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Map<String, Object>> lines = drainStream(s, List.of(), 10L, List.of(), true);
        assertThat(lines.size(), equalTo(2));

        Map<String, Object> footer = lines.get(1);
        assertThat(footer.get("is_partial"), equalTo(true));
    }

    public void testEmptyResults() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Map<String, Object>> lines = drainStream(s, List.of(), 10L, List.of());
        assertThat(s.response().status(), equalTo(RestStatus.OK));
        assertThat(lines.size(), equalTo(2));
        assertThat(lines.get(0), hasKey("columns"));
        assertThat(lines.get(1).get("took"), equalTo(10));
    }

    @SuppressWarnings("unchecked")
    public void testDropNullColumns() throws IOException {
        List<ColumnInfoImpl> allColumns = List.of(
            new ColumnInfoImpl("id", "integer", null),
            new ColumnInfoImpl("tags", "keyword", null),
            new ColumnInfoImpl("name", "keyword", null)
        );
        boolean[] nullColumns = { false, true, false };
        Subscribed s = subscribe(allColumns, nullColumns);

        Block idBlock = blockFactory.newIntArrayVector(new int[] { 7 }, 1).asBlock();
        BytesRefBlock.Builder tagsBuilder = blockFactory.newBytesRefBlockBuilder(1);
        tagsBuilder.appendNull();
        Block tagsBlock = tagsBuilder.build();
        BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(1);
        nameBuilder.appendBytesRef(new BytesRef("bob"));
        Block nameBlock = nameBuilder.build();
        Page page = new Page(idBlock, tagsBlock, nameBlock);
        s.producer().addPage(page);

        ChunkedRestResponseBodyPart columnsPart = s.response().chunkedContent();
        Map<String, Object> columnsMap = decodeLine(columnsPart);
        List<Map<String, Object>> allCols = (List<Map<String, Object>>) columnsMap.get("all_columns");
        assertThat(allCols.size(), equalTo(3));

        List<Map<String, Object>> visibleCols = (List<Map<String, Object>>) columnsMap.get("columns");
        assertThat(visibleCols.size(), equalTo(2));
        assertThat(visibleCols.get(0).get("name"), equalTo("id"));
        assertThat(visibleCols.get(1).get("name"), equalTo("name"));

        ChunkedRestResponseBodyPart pagePart = nextPart(columnsPart, () -> {});

        Map<String, Object> valuesMap = decodeLine(pagePart);
        List<List<Object>> values = (List<List<Object>>) valuesMap.get("values");
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).size(), equalTo(2));
        assertThat(values.get(0), equalTo(List.of(7, "bob")));

        ChunkedRestResponseBodyPart footerPart = nextPart(pagePart, () -> {
            s.producer().finish();
            s.publisher().completeWithFooter(0L, List.of(), false);
        });
        encodeBodyPart(footerPart);
    }

    public void testOnFailureRuntimeException() throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));
        listener.onFailure(new RuntimeException("exception"));

        RestResponse restResponse = channel.capturedResponse();
        assertThat(restResponse.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(channel.errors().get(), equalTo(1));

        assertErrorLine(restResponse.chunkedContent(), 500, "runtime_exception", "exception");
        assertTrue("error part should be the last part", restResponse.chunkedContent().isLastPart());
    }

    public void testOnFailureElasticsearchStatusException() throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));
        listener.onFailure(new ElasticsearchStatusException("not allowed", RestStatus.FORBIDDEN));

        RestResponse restResponse = channel.capturedResponse();
        assertThat(restResponse.status(), equalTo(RestStatus.FORBIDDEN));

        assertErrorLine(restResponse.chunkedContent(), 403, "status_exception", null);
    }

    public void testErrorTypeIsCanonicalExceptionNameAfterUnwrapping() throws IOException {
        ElasticsearchStatusException cause = new ElasticsearchStatusException("not allowed", RestStatus.FORBIDDEN);
        RemoteTransportException wrapper = new RemoteTransportException("node/action", cause);

        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));
        listener.onFailure(wrapper);

        RestResponse restResponse = channel.capturedResponse();
        assertThat(restResponse.status(), equalTo(RestStatus.FORBIDDEN));

        String expectedType = ElasticsearchException.getExceptionName(ExceptionsHelper.unwrapCause(wrapper));
        assertErrorLine(restResponse.chunkedContent(), 403, expectedType, null);
    }

    public void testFailStreamMidStream() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, buildSimplePage(1, "first"));
        encodeBodyPart(pagePart);
        ChunkedRestResponseBodyPart errorPart = nextPart(pagePart, () -> s.publisher().failStream(new RuntimeException("compute failed")));

        assertErrorLine(errorPart, 500, "runtime_exception", "compute failed");
        assertTrue("error part should be the last part", errorPart.isLastPart());
    }

    public void testFailStreamNoContinuationOutstanding() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, buildSimplePage(1, "first"));
        assertFalse("page part must not be the last part before the error arrives", pagePart.isLastPart());
        s.publisher().failStream(new RuntimeException("compute failed mid-write"));
        assertFalse("page part must still not be the last part after failStream", pagePart.isLastPart());
        encodeBodyPart(pagePart);
        ChunkedRestResponseBodyPart errorPart = nextPart(pagePart, () -> {});
        assertErrorLine(errorPart, 500, "runtime_exception", "compute failed mid-write");
        assertTrue("error part should be the last part", errorPart.isLastPart());
    }

    public void testDoubleTerminalEmitsOnce() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, buildSimplePage(1, "first"));
        encodeBodyPart(pagePart);
        ChunkedRestResponseBodyPart errorPart = nextPart(pagePart, () -> s.publisher().failStream(new RuntimeException("first failure")));
        assertErrorLine(errorPart, 500, "runtime_exception", "first failure");
        assertTrue("error part should be the last part", errorPart.isLastPart());
        s.listener().onFailure(new RuntimeException("second failure"));
        assertThat(s.channel().capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testInFlightPageIsReleasedWhenChannelDies() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, buildSimplePage(1, "first"));
        assertNotNull(pagePart);
        s.response().close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testInFlightPageNotDoubleReleasedAfterEncode() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, buildSimplePage(2, "bob"));
        assertNotNull(pagePart);
        encodeBodyPart(pagePart);
        s.response().close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    @SuppressWarnings("unchecked")
    public void testLargePageSplitsAcrossChunks() throws IOException {
        final int rowCount = 50;
        Subscribed s = subscribe(simpleColumns(), null, rowCount);

        final int valueLength = 300;
        Page page = buildLargePage(rowCount, valueLength);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, page);

        List<ReleasableBytesReference> refs = new ArrayList<>();
        int chunkCount = 0;
        while (pagePart.isPartComplete() == false) {
            refs.add(pagePart.encodeChunk(1024, BytesRefRecycler.NON_RECYCLING_INSTANCE));
            chunkCount++;
        }
        assertThat("large page must encode in more than one chunk", chunkCount, greaterThan(1));

        String json = CompositeBytesReference.of(refs.toArray(new BytesReference[0])).utf8ToString().strip();
        Map<String, Object> parsed = parseJson(json);
        refs.forEach(ReleasableBytesReference::close);

        List<List<Object>> values = (List<List<Object>>) parsed.get("values");
        assertThat("all rows must be present after multi-chunk reassembly", values.size(), equalTo(rowCount));
        for (int i = 0; i < rowCount; i++) {
            assertThat("row " + i + " must have the correct id", values.get(i).get(0), equalTo(i));
        }

        nextPart(pagePart, () -> {
            s.producer().finish();
            s.publisher().completeWithFooter(0L, List.of(), false);
        });
        s.response().close();
    }

    public void testReleaseAfterPartialEncodeThrows() throws IOException {
        final int rowCount = 50;
        Subscribed s = subscribe(simpleColumns(), null, rowCount);

        Page page = buildLargePage(rowCount, 300);
        ChunkedRestResponseBodyPart pagePart = sendFirstPage(s, page);
        assertFalse("page part must not be complete before any encoding", pagePart.isPartComplete());

        pagePart.encodeChunk(1024, BytesRefRecycler.NON_RECYCLING_INSTANCE).close();
        assertFalse("page part must still be incomplete after one partial chunk", pagePart.isPartComplete());

        s.response().close();
        assertThat("all page blocks must be released after close", blockFactory.breaker().getUsed(), equalTo(0L));

        expectThrows(IllegalStateException.class, () -> pagePart.encodeChunk(1024, BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    public void testSubscribeBeforeSendResponseClosesEventLoopRace() throws IOException {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        EarlyGetNextPartChannel channel = new EarlyGetNextPartChannel();
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));

        listener.resultStreamListener()
            .onResponse(new EsqlStreamQueryAction.ResultStream(simpleColumns(), publisher, null, ZoneOffset.UTC));
        assertThat("no response before the first page", channel.okResponses, equalTo(0));
        assertThat(channel.errorResponses, equalTo(0));
        assertTrue("publisher should be unblocked after early demand", publisher.waitForWriting().listener().isDone());
        assertNull("no part should be delivered before a page is added", channel.earlyPart.get());

        Page page = buildSimplePage(7, "carol");
        producer.addPage(page);
        assertThat("first page must send the response", channel.okResponses, equalTo(1));
        assertThat(channel.errorResponses, equalTo(0));
        ChunkedRestResponseBodyPart pagePart = channel.earlyPart.get();
        assertNotNull("a getNextPart issued inside sendResponse must receive the stashed first page", pagePart);
        encodeBodyPart(pagePart);
    }

    public void testFailedFirstSendMustNotLeaveTheProducerBlocked() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        ThrowingOkChannel channel = new ThrowingOkChannel();
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));

        listener.resultStreamListener()
            .onResponse(new EsqlStreamQueryAction.ResultStream(simpleColumns(), publisher, null, ZoneOffset.UTC));
        assertThat("no 200 OK before the first page", channel.okAttempts, equalTo(0));

        producer.addPage(buildSimplePage(1, "first"));
        assertThat("the 200 OK write must be attempted exactly once", channel.okAttempts, equalTo(1));
        assertThat("exactly one error response must be sent", channel.errorResponses, equalTo(1));
        assertNotNull("publisher must be terminalized after a failed send", publisher.failure());
        assertTrue(
            "publisher gate must be open after a failed send so the driver is not stuck",
            publisher.waitForWriting().listener().isDone()
        );
        assertThat("all blocks must be released after a failed send", blockFactory.breaker().getUsed(), equalTo(0L));

        listener.onFailure(new RuntimeException("compute failed after bad send"));
        assertThat(channel.errorResponses, equalTo(1));
    }

    private static class ThrowingOkChannel extends AbstractRestChannel {
        int okAttempts;
        int errorResponses;

        ThrowingOkChannel() {
            super(new FakeRestRequest(), true);
        }

        @Override
        public void sendResponse(RestResponse response) {
            if (response.status() == RestStatus.OK) {
                okAttempts++;
                throw new RuntimeException("simulated channel failure on 200 OK write");
            } else {
                errorResponses++;
            }
        }
    }

    private static class EarlyGetNextPartChannel extends AbstractRestChannel {
        final AtomicReference<ChunkedRestResponseBodyPart> earlyPart = new AtomicReference<>();
        int okResponses;
        int errorResponses;

        EarlyGetNextPartChannel() {
            super(new FakeRestRequest(), true);
        }

        @Override
        public void sendResponse(RestResponse response) {
            if (response.status() == RestStatus.OK) {
                okResponses++;
            } else {
                errorResponses++;
            }
            if (response.isChunked() && response.status() == RestStatus.OK) {
                response.chunkedContent().getNextPart(ActionListener.wrap(earlyPart::set, e -> fail("unexpected failure: " + e)));
            }
        }
    }

    private record Subscribed(
        PageStreamPublisher publisher,
        PageStreamPublisher.Producer producer,
        FakeRestChannel channel,
        EsqlStreamResponseListener listener
    ) {
        /** Null until the first page, footer or error has been delivered. */
        RestResponse response() {
            return channel.capturedResponse();
        }
    }

    private Subscribed subscribe(List<ColumnInfoImpl> columns, boolean[] nullColumns) {
        return subscribe(columns, nullColumns, 1);
    }

    private Subscribed subscribe(List<ColumnInfoImpl> columns, boolean[] nullColumns, int pageSize) {
        return subscribe(columns, nullColumns, pageSize, ZoneOffset.UTC);
    }

    private Subscribed subscribe(List<ColumnInfoImpl> columns, boolean[] nullColumns, int pageSize, ZoneId zoneId) {
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel, new ThreadContext(Settings.EMPTY));
        listener.resultStreamListener().onResponse(new EsqlStreamQueryAction.ResultStream(columns, publisher, nullColumns, zoneId));
        return new Subscribed(publisher, producer, channel, listener);
    }

    private ChunkedRestResponseBodyPart sendFirstPage(Subscribed s, Page page) throws IOException {
        s.producer().addPage(page);
        RestResponse response = s.response();
        assertNotNull("the first page must send the response", response);
        assertThat(response.status(), equalTo(RestStatus.OK));
        ChunkedRestResponseBodyPart columnsPart = response.chunkedContent();
        encodeBodyPart(columnsPart);
        return nextPart(columnsPart, () -> {});
    }

    private static ChunkedRestResponseBodyPart nextPart(ChunkedRestResponseBodyPart current, Runnable trigger) {
        AtomicReference<ChunkedRestResponseBodyPart> ref = new AtomicReference<>();
        current.getNextPart(ActionListener.wrap(ref::set, e -> fail("unexpected failure in getNextPart: " + e)));
        trigger.run();
        ChunkedRestResponseBodyPart next = ref.get();
        assertNotNull("body part must be delivered synchronously", next);
        return next;
    }

    private Map<String, Object> decodeLine(ChunkedRestResponseBodyPart part) throws IOException {
        return parseJson(encodeBodyPart(part).utf8ToString().strip());
    }

    private void assertErrorLine(ChunkedRestResponseBodyPart part, int status, String type, String reason) throws IOException {
        Map<String, Object> line = decodeLine(part);
        assertThat(line.get("status"), equalTo(status));
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) line.get("error");
        assertThat(error.get("type"), equalTo(type));
        if (reason != null) {
            assertThat(error.get("reason"), equalTo(reason));
        } else {
            assertNotNull(error.get("reason"));
        }
    }

    public void testNoResponseBeforeFirstPage() {
        Subscribed s = subscribe(simpleColumns(), null);
        assertNull("initializing the stream alone must not send the response", s.response());
        assertTrue("init must request the first page", s.publisher().waitForWriting().listener().isDone());
    }

    public void testFailStreamBeforeFirstPageSendsHttpError() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        s.publisher().failStream(new ElasticsearchStatusException("not allowed", RestStatus.FORBIDDEN));
        RestResponse response = s.response();
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(s.channel().responses().get(), equalTo(0));
        assertErrorLine(response.chunkedContent(), 403, "status_exception", null);
        assertTrue("the error footer must be the only part", response.chunkedContent().isLastPart());
    }

    public void testFailStreamBeforeFirstPageUsesTransportFooter() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ElasticsearchStatusException e = new ElasticsearchStatusException("too many", RestStatus.TOO_MANY_REQUESTS);
        s.publisher().failStream(e, new PageStreamPublisher.StreamFooter(429, 17L, false, List.of("w1"), null, e));
        RestResponse response = s.response();
        assertThat(response.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        Map<String, Object> line = decodeLine(response.chunkedContent());
        assertThat(line.get("status"), equalTo(429));
        assertThat(line.get("took"), equalTo(17));
        assertThat(line.get("warnings"), equalTo(List.of("w1")));
        assertThat(line, hasKey("error"));
    }

    public void testFailStreamWhileFirstPageIsStashed() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        s.producer().addPage(buildSimplePage(1, "first"));
        s.publisher().failStream(new RuntimeException("compute failed"));
        RestResponse response = s.response();
        assertThat(response.status(), equalTo(RestStatus.OK));
        ChunkedRestResponseBodyPart columnsPart = response.chunkedContent();
        assertThat(decodeLine(columnsPart), hasKey("columns"));
        ChunkedRestResponseBodyPart pagePart = nextPart(columnsPart, () -> {});
        assertThat(decodeLine(pagePart), hasKey("values"));
        ChunkedRestResponseBodyPart errorPart = nextPart(pagePart, () -> {});
        assertErrorLine(errorPart, 500, "runtime_exception", "compute failed");
        assertTrue(errorPart.isLastPart());
    }

    public void testChannelClosedWithStashedPageReleasesBlocks() {
        Subscribed s = subscribe(simpleColumns(), null);
        s.producer().addPage(buildSimplePage(1, "first"));
        s.response().close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testDatetimeValuesUseTheQueryTimeZone() throws IOException {
        long epochMillis = 1748649600123L; // 2025-05-31T00:00:00.123Z
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("ts", DataType.DATETIME, null));
        ZoneId paris = ZoneId.of("Europe/Paris");
        Subscribed s = subscribe(columns, null, 1, paris);

        LongBlock longBlock = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
        Page page = new Page(longBlock);

        List<Map<String, Object>> lines = drainStream(s, List.of(page), 0L, List.of());
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) lines.get(1).get("values");
        assertNotNull("values line must be present", rows);
        assertThat(rows.size(), greaterThan(0));
        assertThat("datetime must be formatted with the query time zone", rows.get(0).get(0), equalTo("2025-05-31T02:00:00.123+02:00"));
    }

    public void testDateNanosValuesUseTheQueryTimeZone() throws IOException {
        long epochNanos = 1748649600123456789L; // 2025-05-31T00:00:00.123456789Z
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("ts", DataType.DATE_NANOS, null));
        ZoneId paris = ZoneId.of("Europe/Paris");
        Subscribed s = subscribe(columns, null, 1, paris);

        LongBlock longBlock = blockFactory.newLongArrayVector(new long[] { epochNanos }, 1).asBlock();
        Page page = new Page(longBlock);

        List<Map<String, Object>> lines = drainStream(s, List.of(page), 0L, List.of());
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) lines.get(1).get("values");
        assertNotNull("values line must be present", rows);
        assertThat(rows.size(), greaterThan(0));
        assertThat(
            "date_nanos must be formatted with the query time zone",
            rows.get(0).get(0),
            equalTo("2025-05-31T02:00:00.123456789+02:00")
        );
    }

    private static List<ColumnInfoImpl> simpleColumns() {
        return List.of(new ColumnInfoImpl("id", "integer", null), new ColumnInfoImpl("name", "keyword", null));
    }

    private Page buildSimplePage(int id, String name) {
        Block idBlock = blockFactory.newIntArrayVector(new int[] { id }, 1).asBlock();
        BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(1);
        nameBuilder.appendBytesRef(new BytesRef(name));
        Block nameBlock = nameBuilder.build();
        return new Page(idBlock, nameBlock);
    }

    private Page buildLargePage(int rowCount, int valueLength) {
        int[] ids = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            ids[i] = i;
        }
        Block idBlock = blockFactory.newIntArrayVector(ids, rowCount).asBlock();
        BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
        byte[] value = new byte[valueLength];
        Arrays.fill(value, (byte) 'x');
        BytesRef valueRef = new BytesRef(value);
        for (int i = 0; i < rowCount; i++) {
            nameBuilder.appendBytesRef(valueRef);
        }
        return new Page(idBlock, nameBuilder.build());
    }

    private static BytesReference encodeBodyPart(ChunkedRestResponseBodyPart part) throws IOException {
        List<BytesReference> refs = new ArrayList<>();
        while (part.isPartComplete() == false) {
            refs.add(part.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        return CompositeBytesReference.of(refs.toArray(new BytesReference[0]));
    }

    private List<Map<String, Object>> drainStream(Subscribed s, List<Page> pages, long tookMillis, List<String> warnings)
        throws IOException {
        return drainStream(s, pages, tookMillis, warnings, false);
    }

    private List<Map<String, Object>> drainStream(Subscribed s, List<Page> pages, long tookMillis, List<String> warnings, boolean isPartial)
        throws IOException {
        Runnable finish = () -> {
            s.producer().finish();
            s.publisher().completeWithFooter(tookMillis, warnings, isPartial);
        };
        if (pages.isEmpty()) {
            finish.run();
        } else {
            s.producer().addPage(pages.get(0));
        }
        RestResponse restResponse = s.response();
        assertNotNull("the response must be sent once the first page or the footer arrives", restResponse);
        assertTrue("expected a chunked response", restResponse.isChunked());
        ChunkedRestResponseBodyPart currentPart = restResponse.chunkedContent();

        List<Map<String, Object>> lines = new ArrayList<>();
        lines.add(decodeLine(currentPart));
        assertFalse("columns part must not be the last part", currentPart.isLastPart());

        if (pages.isEmpty() == false) {
            currentPart = nextPart(currentPart, () -> {});
            lines.add(decodeLine(currentPart));
            assertFalse("page body part must not be the last part yet", currentPart.isLastPart());
            for (Page page : pages.subList(1, pages.size())) {
                currentPart = nextPart(currentPart, () -> s.producer().addPage(page));
                lines.add(decodeLine(currentPart));
                assertFalse("page body part must not be the last part yet", currentPart.isLastPart());
            }
        }

        ChunkedRestResponseBodyPart footerPart = nextPart(currentPart, pages.isEmpty() ? () -> {} : finish);
        assertTrue("footer must be the last part", footerPart.isLastPart());
        lines.add(decodeLine(footerPart));

        return lines;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(String json) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
