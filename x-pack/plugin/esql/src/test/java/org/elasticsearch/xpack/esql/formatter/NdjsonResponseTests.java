/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests {@link NdjsonResponse}, the non-streaming rendering of {@link NdjsonFormat}. Each test drains the body with a randomly
 * chosen {@code sizeHint}, so every assertion also checks that the output does not depend on how it is chunked.
 */
public class NdjsonResponseTests extends ESTestCase {
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

    public void testFramingAndFooter() throws IOException {
        EsqlQueryResponse response = response(List.of(page(0, 3)), noMetadata(), 10L, 20L, 3L, 40L, 50L, 60L, 70L);
        try {
            List<Map<String, Object>> lines = render(response, 100, false, List.of());
            assertThat(lines.size(), equalTo(3));

            assertThat(lines.get(0), equalTo(Map.of("columns", columnMaps(simpleColumns()))));
            assertThat(rows(lines.get(1)), equalTo(List.of(List.of(0, "row0"), List.of(1, "row1"), List.of(2, "row2"))));

            Map<String, Object> footer = lines.get(2);
            assertThat(footer.get("status"), equalTo(200));
            assertThat(footer.get("took"), instanceOf(Number.class));
            assertThat(footer.get("is_partial"), equalTo(false));
            assertThat(footer.get("warnings"), equalTo(List.of()));
            assertThat(footer.get("documents_found"), equalTo(10));
            assertThat(footer.get("values_loaded"), equalTo(20));
            assertThat(footer.get("rows_emitted"), equalTo(3));
            assertThat(footer.get("bytes_read"), equalTo(40));
            assertThat(footer.get("read_nanos"), equalTo(50));
            assertThat("the footer has the same shape as the streaming footer", footer, not(hasKey("read_cpu_nanos")));
            assertThat(footer.get("cpu_nanos"), equalTo(70));
            assertThat(footer, not(hasKey("error")));
            assertThat(footer, not(hasKey("profile")));
            assertThat(footer, not(hasKey("_clusters")));
        } finally {
            response.decRef();
        }
    }

    public void testBatchSizeIsIndependentOfPageBoundaries() throws IOException {
        EsqlQueryResponse response = response(List.of(page(0, 3), page(3, 4)), noMetadata());
        try {
            List<Map<String, Object>> lines = render(response, 5, false, List.of());
            assertThat(lines.size(), equalTo(4));
            assertThat(rows(lines.get(1)).size(), equalTo(5));
            assertThat(rows(lines.get(2)).size(), equalTo(2));

            List<Object> ids = new ArrayList<>();
            for (List<Object> row : rows(lines.get(1))) {
                ids.add(row.get(0));
            }
            for (List<Object> row : rows(lines.get(2))) {
                ids.add(row.get(0));
            }
            assertThat("rows keep their order across pages", ids, equalTo(List.of(0, 1, 2, 3, 4, 5, 6)));
        } finally {
            response.decRef();
        }
    }

    public void testNoTrailingEmptyValuesLine() throws IOException {
        EsqlQueryResponse response = response(List.of(page(0, 3), page(3, 3)), noMetadata());
        try {
            List<Map<String, Object>> lines = render(response, 3, false, List.of());
            assertThat("columns, two full values lines, footer", lines.size(), equalTo(4));
            assertThat(rows(lines.get(1)).size(), equalTo(3));
            assertThat(rows(lines.get(2)).size(), equalTo(3));
        } finally {
            response.decRef();
        }
    }

    public void testBatchSizeOfOne() throws IOException {
        EsqlQueryResponse response = response(List.of(page(0, 4)), noMetadata());
        try {
            List<Map<String, Object>> lines = render(response, 1, false, List.of());
            assertThat(lines.size(), equalTo(6));
            for (int i = 1; i <= 4; i++) {
                assertThat(rows(lines.get(i)).size(), equalTo(1));
            }
        } finally {
            response.decRef();
        }
    }

    public void testEmptyResult() throws IOException {
        EsqlQueryResponse response = response(List.of(), noMetadata());
        try {
            List<Map<String, Object>> lines = render(response, 100, false, List.of());
            assertThat("only the columns line and the footer", lines.size(), equalTo(2));
            assertThat(lines.get(0), hasKey("columns"));
            assertThat(lines.get(1).get("status"), equalTo(200));
        } finally {
            response.decRef();
        }
    }

    @SuppressWarnings("unchecked")
    public void testDropNullColumns() throws IOException {
        List<ColumnInfoImpl> columns = List.of(
            new ColumnInfoImpl("id", "integer", null),
            new ColumnInfoImpl("name", "keyword", null),
            new ColumnInfoImpl("empty", "keyword", null)
        );
        Block id = blockFactory.newIntArrayVector(new int[] { 1, 2 }, 2).asBlock();
        BytesRefBlock.Builder name = blockFactory.newBytesRefBlockBuilder(2);
        name.appendBytesRef(new BytesRef("a"));
        name.appendBytesRef(new BytesRef("b"));
        Page page = new Page(id, name.build(), blockFactory.newConstantNullBlock(2));
        EsqlQueryResponse response = new EsqlQueryResponse(
            columns,
            List.of(page),
            0L,
            0L,
            null,
            false,
            false,
            ZoneOffset.UTC,
            0L,
            0L,
            noMetadata()
        );
        try {
            List<Map<String, Object>> lines = render(response, 100, true, List.of());
            assertThat(((List<Map<String, Object>>) lines.get(0).get("all_columns")).size(), equalTo(3));
            assertThat(((List<Map<String, Object>>) lines.get(0).get("columns")).size(), equalTo(2));
            assertThat(rows(lines.get(1)), equalTo(List.of(List.of(1, "a"), List.of(2, "b"))));
        } finally {
            response.decRef();
        }
    }

    public void testWithoutDropNullColumnsHeaderHasNoAllColumns() throws IOException {
        EsqlQueryResponse response = response(List.of(page(0, 1)), noMetadata());
        try {
            assertThat(render(response, 100, false, List.of()).get(0), not(hasKey("all_columns")));
        } finally {
            response.decRef();
        }
    }

    public void testWarningsAndPartialInFooter() throws IOException {
        EsqlExecutionInfo executionInfo = noMetadata();
        executionInfo.markPartial();
        EsqlQueryResponse response = response(List.of(page(0, 1)), executionInfo);
        try {
            List<Map<String, Object>> lines = render(response, 100, false, List.of("warning1", "warning2"));
            Map<String, Object> footer = lines.get(lines.size() - 1);
            assertThat(footer.get("warnings"), equalTo(List.of("warning1", "warning2")));
            assertThat(footer.get("is_partial"), equalTo(true));
        } finally {
            response.decRef();
        }
    }

    public void testClustersInFooter() throws IOException {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> true, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        executionInfo.initCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "test-cluster", "index");
        EsqlQueryResponse response = response(List.of(page(0, 1)), executionInfo);
        try {
            List<Map<String, Object>> lines = render(response, 100, false, List.of());
            assertThat(lines.get(lines.size() - 1), hasKey("_clusters"));
        } finally {
            response.decRef();
        }
    }

    public void testProfileInFooter() throws IOException {
        EsqlQueryResponse response = new EsqlQueryResponse(
            simpleColumns(),
            List.of(page(0, 1)),
            0L,
            0L,
            new EsqlQueryResponse.Profile(List.of(), List.of(), null),
            false,
            false,
            ZoneOffset.UTC,
            0L,
            0L,
            noMetadata()
        );
        try {
            List<Map<String, Object>> lines = render(response, 100, false, List.of());
            Map<String, Object> footer = lines.get(lines.size() - 1);
            assertThat(footer, hasKey("profile"));
            @SuppressWarnings("unchecked")
            Map<String, Object> profile = (Map<String, Object>) footer.get("profile");
            assertThat(profile, hasKey("drivers"));
            assertThat(profile, hasKey("plans"));
        } finally {
            response.decRef();
        }
    }

    public void testValuesLineSpansChunks() throws IOException {
        final int rowCount = 50;
        EsqlQueryResponse response = response(List.of(largePage(rowCount, 300)), noMetadata());
        try {
            NdjsonResponse part = ndjsonResponse(response, rowCount, false, List.of());
            List<ReleasableBytesReference> refs = new ArrayList<>();
            while (part.isPartComplete() == false) {
                refs.add(part.encodeChunk(1024, BytesRefRecycler.NON_RECYCLING_INSTANCE));
            }
            assertThat("one long values line must be split across chunks", refs.size(), greaterThan(2));
            String body = CompositeBytesReference.of(refs.toArray(new BytesReference[0])).utf8ToString();
            refs.forEach(ReleasableBytesReference::close);

            List<Map<String, Object>> lines = parse(body);
            assertThat(lines.size(), equalTo(3));
            assertThat(rows(lines.get(1)).size(), equalTo(rowCount));
        } finally {
            response.decRef();
        }
    }

    public void testPartMetadata() {
        EsqlQueryResponse response = response(List.of(), noMetadata());
        try {
            NdjsonResponse part = ndjsonResponse(response, 100, false, List.of());
            assertFalse(part.isPartComplete());
            assertTrue("the whole result is one part", part.isLastPart());
            assertThat(part.getResponseContentTypeString(), equalTo("application/x-ndjson"));
        } finally {
            response.decRef();
        }
    }

    public void testFormatIsNotRegisteredForAnyHeader() {
        assertThat(NdjsonFormat.INSTANCE.queryParameter(), equalTo("ndjson"));
        assertTrue(
            "registering a header would reassign Accept: application/x-ndjson, which must keep returning JSON",
            NdjsonFormat.INSTANCE.headerValues().isEmpty()
        );
    }

    private NdjsonResponse ndjsonResponse(EsqlQueryResponse response, int batchSize, boolean dropNullColumns, List<String> warnings) {
        return new NdjsonResponse(response, batchSize, dropNullColumns, warnings, ToXContent.EMPTY_PARAMS);
    }

    /** Renders the whole body, chunked with a random {@code sizeHint}, and parses each line. */
    private List<Map<String, Object>> render(EsqlQueryResponse response, int batchSize, boolean dropNullColumns, List<String> warnings)
        throws IOException {
        NdjsonResponse part = ndjsonResponse(response, batchSize, dropNullColumns, warnings);
        List<ReleasableBytesReference> refs = new ArrayList<>();
        while (part.isPartComplete() == false) {
            refs.add(part.encodeChunk(randomFrom(1, 7, 64, 4096, Integer.MAX_VALUE), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        String body = CompositeBytesReference.of(refs.toArray(new BytesReference[0])).utf8ToString();
        refs.forEach(ReleasableBytesReference::close);
        assertTrue("every line ends with a newline", body.endsWith("\n"));
        return parse(body);
    }

    private static List<Map<String, Object>> parse(String body) throws IOException {
        List<Map<String, Object>> lines = new ArrayList<>();
        for (String line : body.split("\n")) {
            assertFalse("no blank lines", line.isBlank());
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line)) {
                lines.add(parser.map());
            }
        }
        return lines;
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> valuesLine) {
        assertThat(valuesLine, hasKey("values"));
        return (List<List<Object>>) valuesLine.get("values");
    }

    private static List<Map<String, Object>> columnMaps(List<ColumnInfoImpl> columns) {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (ColumnInfoImpl column : columns) {
            maps.add(Map.of("name", column.name(), "type", column.outputType()));
        }
        return maps;
    }

    private static EsqlExecutionInfo noMetadata() {
        return new EsqlExecutionInfo(alias -> true, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
    }

    private static List<ColumnInfoImpl> simpleColumns() {
        return List.of(new ColumnInfoImpl("id", "integer", null), new ColumnInfoImpl("name", "keyword", null));
    }

    private static EsqlQueryResponse response(List<Page> pages, EsqlExecutionInfo executionInfo) {
        return new EsqlQueryResponse(simpleColumns(), pages, 0L, 0L, null, false, false, ZoneOffset.UTC, 0L, 0L, executionInfo);
    }

    private static EsqlQueryResponse response(
        List<Page> pages,
        EsqlExecutionInfo executionInfo,
        long documentsFound,
        long valuesLoaded,
        long rowsEmitted,
        long bytesRead,
        long readNanos,
        long readCpuNanos,
        long cpuNanos
    ) {
        return new EsqlQueryResponse(
            simpleColumns(),
            pages,
            documentsFound,
            valuesLoaded,
            rowsEmitted,
            bytesRead,
            readNanos,
            readCpuNanos,
            cpuNanos,
            null,
            false,
            null,
            false,
            false,
            ZoneOffset.UTC,
            0L,
            0L,
            executionInfo,
            null
        );
    }

    /** A page of {@code rows} rows whose ids count up from {@code firstId} and whose names are {@code row<id>}. */
    private Page page(int firstId, int rows) {
        int[] ids = new int[rows];
        BytesRefBlock.Builder names = blockFactory.newBytesRefBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            ids[i] = firstId + i;
            names.appendBytesRef(new BytesRef("row" + (firstId + i)));
        }
        return new Page(blockFactory.newIntArrayVector(ids, rows).asBlock(), names.build());
    }

    private Page largePage(int rows, int valueLength) {
        int[] ids = new int[rows];
        for (int i = 0; i < rows; i++) {
            ids[i] = i;
        }
        byte[] value = new byte[valueLength];
        Arrays.fill(value, (byte) 'x');
        BytesRef valueRef = new BytesRef(value);
        BytesRefBlock.Builder names = blockFactory.newBytesRefBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            names.appendBytesRef(valueRef);
        }
        return new Page(blockFactory.newIntArrayVector(ids, rows).asBlock(), names.build());
    }
}
