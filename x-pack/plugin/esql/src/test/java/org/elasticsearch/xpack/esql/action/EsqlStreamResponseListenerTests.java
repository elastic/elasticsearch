/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
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
        assertNotNull(s.response());
        assertThat(s.response().status(), equalTo(RestStatus.OK));
        assertThat(s.response().contentType(), equalTo("application/x-ndjson"));

        List<Map<String, Object>> lines = drainStream(s.response(), s.publisher(), List.of(buildSimplePage(42, "alice")), 100L, List.of());
        assertThat(lines.size(), equalTo(3));

        List<Map<String, Object>> cols = (List<Map<String, Object>>) lines.get(0).get("columns");
        assertThat(cols.size(), equalTo(2));
        assertThat(cols.get(0), equalTo(Map.of("name", "id", "type", "integer")));
        assertThat(cols.get(1), equalTo(Map.of("name", "name", "type", "keyword")));

        List<List<Object>> values = (List<List<Object>>) lines.get(1).get("values");
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0), equalTo(List.of(42, "alice")));

        assertThat(lines.get(2), equalTo(Map.of("took", 100, "is_partial", false)));
    }

    @SuppressWarnings("unchecked")
    public void testMultiplePages() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Page> pages = List.of(buildSimplePage(1, "a"), buildSimplePage(2, "b"), buildSimplePage(3, "c"));
        List<Map<String, Object>> lines = drainStream(s.response(), s.publisher(), pages, 50L, List.of());
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
        List<Map<String, Object>> lines = drainStream(s.response(), s.publisher(), List.of(), 42L, List.of("warning1", "warning2"));
        assertThat(lines.size(), equalTo(2));

        Map<String, Object> footer = lines.get(1);
        assertThat(footer, equalTo(Map.of("took", 42, "is_partial", false, "warnings", List.of("warning1", "warning2"))));
    }

    public void testFooterIsPartial() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Map<String, Object>> lines = drainStream(s.response(), s.publisher(), List.of(), 10L, List.of(), true);
        assertThat(lines.size(), equalTo(2));

        Map<String, Object> footer = lines.get(1);
        assertThat(footer.get("is_partial"), equalTo(true));
    }

    public void testEmptyResults() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        List<Map<String, Object>> lines = drainStream(s.response(), s.publisher(), List.of(), 10L, List.of());
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

        ChunkedRestResponseBodyPart columnsPart = s.response().chunkedContent();
        Map<String, Object> columnsMap = decodeLine(columnsPart);
        List<Map<String, Object>> allCols = (List<Map<String, Object>>) columnsMap.get("all_columns");
        assertThat(allCols.size(), equalTo(3));

        List<Map<String, Object>> visibleCols = (List<Map<String, Object>>) columnsMap.get("columns");
        assertThat(visibleCols.size(), equalTo(2));
        assertThat(visibleCols.get(0).get("name"), equalTo("id"));
        assertThat(visibleCols.get(1).get("name"), equalTo("name"));

        Block idBlock = blockFactory.newIntArrayVector(new int[] { 7 }, 1).asBlock();
        BytesRefBlock.Builder tagsBuilder = blockFactory.newBytesRefBlockBuilder(1);
        tagsBuilder.appendNull();
        Block tagsBlock = tagsBuilder.build();
        BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(1);
        nameBuilder.appendBytesRef(new BytesRef("bob"));
        Block nameBlock = nameBuilder.build();
        Page page = new Page(idBlock, tagsBlock, nameBlock);

        ChunkedRestResponseBodyPart pagePart = nextPart(columnsPart, () -> s.publisher().addPage(page));

        Map<String, Object> valuesMap = decodeLine(pagePart);
        List<List<Object>> values = (List<List<Object>>) valuesMap.get("values");
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).size(), equalTo(2));
        assertThat(values.get(0), equalTo(List.of(7, "bob")));

        ChunkedRestResponseBodyPart footerPart = nextPart(pagePart, () -> {
            s.publisher().pagesFinished();
            s.publisher().completeWithFooter(0L, List.of(), false);
        });
        encodeBodyPart(footerPart);
    }

    public void testOnFailureRuntimeException() throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel);
        listener.onFailure(new RuntimeException("exception"));

        RestResponse restResponse = channel.capturedResponse();
        assertThat(restResponse.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(channel.errors().get(), equalTo(1));

        assertErrorLine(restResponse.chunkedContent(), 500, "RuntimeException", "exception");
        assertTrue("error part should be the last part", restResponse.chunkedContent().isLastPart());
    }

    public void testOnFailureElasticsearchStatusException() throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        EsqlStreamResponseListener listener = new EsqlStreamResponseListener(channel);
        listener.onFailure(new ElasticsearchStatusException("not allowed", RestStatus.FORBIDDEN));

        RestResponse restResponse = channel.capturedResponse();
        assertThat(restResponse.status(), equalTo(RestStatus.FORBIDDEN));

        assertErrorLine(restResponse.chunkedContent(), 403, "ElasticsearchStatusException", null);
    }

    public void testFailStreamMidStream() throws IOException {
        Subscribed s = subscribe(simpleColumns(), null);
        ChunkedRestResponseBodyPart currentPart = s.response().chunkedContent();

        encodeBodyPart(currentPart);
        currentPart = nextPart(currentPart, () -> s.publisher().addPage(buildSimplePage(1, "first")));
        encodeBodyPart(currentPart);
        ChunkedRestResponseBodyPart errorPart = nextPart(
            currentPart,
            () -> s.publisher().failStream(new RuntimeException("compute failed"))
        );

        assertErrorLine(errorPart, 500, "RuntimeException", "compute failed");
        assertTrue("error part should be the last part", errorPart.isLastPart());
    }

    private record Subscribed(PageStreamPublisher publisher, FakeRestChannel channel, RestResponse response) {}

    private Subscribed subscribe(List<ColumnInfoImpl> columns, boolean[] nullColumns) {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        new EsqlStreamResponseListener(channel).onResponse(new EsqlStreamQueryAction.Response(columns, publisher, nullColumns));
        return new Subscribed(publisher, channel, channel.capturedResponse());
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

    private static BytesReference encodeBodyPart(ChunkedRestResponseBodyPart part) throws IOException {
        List<BytesReference> refs = new ArrayList<>();
        while (part.isPartComplete() == false) {
            refs.add(part.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        return CompositeBytesReference.of(refs.toArray(new BytesReference[0]));
    }

    private List<Map<String, Object>> drainStream(
        RestResponse restResponse,
        PageStreamPublisher publisher,
        List<Page> pages,
        long tookMillis,
        List<String> warnings
    ) throws IOException {
        return drainStream(restResponse, publisher, pages, tookMillis, warnings, false);
    }

    private List<Map<String, Object>> drainStream(
        RestResponse restResponse,
        PageStreamPublisher publisher,
        List<Page> pages,
        long tookMillis,
        List<String> warnings,
        boolean isPartial
    ) throws IOException {
        assertTrue("expected a chunked response", restResponse.isChunked());
        ChunkedRestResponseBodyPart currentPart = restResponse.chunkedContent();

        List<Map<String, Object>> lines = new ArrayList<>();
        lines.add(decodeLine(currentPart));
        assertFalse("columns part must not be the last part", currentPart.isLastPart());

        for (Page page : pages) {
            currentPart = nextPart(currentPart, () -> publisher.addPage(page));
            lines.add(decodeLine(currentPart));
            assertFalse("page body part must not be the last part yet", currentPart.isLastPart());
        }

        ChunkedRestResponseBodyPart footerPart = nextPart(currentPart, () -> {
            publisher.pagesFinished();
            publisher.completeWithFooter(tookMillis, warnings, isPartial);
        });
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
