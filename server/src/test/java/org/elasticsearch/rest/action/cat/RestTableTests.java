/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.rest.action.cat.RestTable.buildDisplayHeaders;
import static org.elasticsearch.rest.action.cat.RestTable.buildResponse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class RestTableTests extends ESTestCase {

    private static final String APPLICATION_JSON = XContentType.JSON.mediaType();
    private static final String APPLICATION_YAML = XContentType.YAML.mediaType();
    private static final String APPLICATION_SMILE = XContentType.SMILE.mediaType();
    private static final String APPLICATION_CBOR = XContentType.CBOR.mediaType();
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String ACCEPT = "Accept";
    private static final String TEXT_PLAIN = "text/plain; charset=UTF-8";
    private static final String TEXT_TABLE_BODY = "foo foo foo foo foo foo foo foo\n";
    private static final String JSON_TABLE_BODY = """
        [
          {
            "bulk.foo": "foo",
            "bulk.bar": "foo",
            "aliasedBulk": "foo",
            "aliasedSecondBulk": "foo",
            "unmatched": "foo",
            "invalidAliasesBulk": "foo",
            "timestamp": "foo",
            "epoch": "foo"
          }
        ]""";
    private static final String YAML_TABLE_BODY = """
        ---
        - bulk.foo: "foo"
          bulk.bar: "foo"
          aliasedBulk: "foo"
          aliasedSecondBulk: "foo"
          unmatched: "foo"
          invalidAliasesBulk: "foo"
          timestamp: "foo"
          epoch: "foo"
        """;
    private Table table;
    private FakeRestRequest restRequest;

    @Before
    public void setup() {
        restRequest = new FakeRestRequest();
        table = new Table();
        table.startHeaders();
        table.addCell("bulk.foo", "alias:f;desc:foo");
        table.addCell("bulk.bar", "alias:b;desc:bar");
        // should be matched as well due to the aliases
        table.addCell("aliasedBulk", "alias:bulkWhatever;desc:bar");
        table.addCell("aliasedSecondBulk", "alias:foobar,bulkolicious,bulkotastic;desc:bar");
        // no match
        table.addCell("unmatched", "alias:un.matched;desc:bar");
        // invalid alias
        table.addCell("invalidAliasesBulk", "alias:,,,;desc:bar");
        // timestamp
        table.addCell("timestamp", "alias:ts");
        table.addCell("epoch", "alias:t");
        table.endHeaders();
    }

    public void testThatDisplayHeadersSupportWildcards() throws Exception {
        restRequest.params().put("h", "bulk*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatDisplayHeadersAreNotAddedTwice() throws Exception {
        restRequest.params().put("h", "nonexistent,bulk*,bul*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatWeUseTheAcceptHeaderJson() throws Exception {
        assertResponse(
            Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_JSON)),
            APPLICATION_JSON,
            XContentHelper.stripWhitespace(JSON_TABLE_BODY)
        );
    }

    public void testThatWeUseTheAcceptHeaderYaml() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_YAML)), APPLICATION_YAML, YAML_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderSmile() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_SMILE)), APPLICATION_SMILE);
    }

    public void testThatWeUseTheAcceptHeaderCbor() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_CBOR)), APPLICATION_CBOR);
    }

    public void testThatWeUseTheAcceptHeaderText() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, Collections.singletonList(TEXT_PLAIN)), TEXT_PLAIN, TEXT_TABLE_BODY);
    }

    public void testIgnoreContentType() throws Exception {
        assertResponse(Collections.singletonMap(CONTENT_TYPE, Collections.singletonList(APPLICATION_JSON)), TEXT_PLAIN, TEXT_TABLE_BODY);
    }

    public void testThatDisplayHeadersWithoutTimestamp() throws Exception {
        restRequest.params().put("h", "timestamp,epoch,bulk*");
        restRequest.params().put("ts", "false");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("timestamp")));
        assertThat(headerNames, not(hasItem("epoch")));
    }

    public void testCompareRow() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();

        for (Integer i : Arrays.asList(1, 2, 1)) {
            table.startRow();
            table.addCell(i);
            table.endRow();
        }

        RestTable.TableIndexComparator comparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", false))
        );
        assertTrue(comparator.compare(0, 1) < 0);
        assertTrue(comparator.compare(0, 2) == 0);
        assertTrue(comparator.compare(1, 2) > 0);

        RestTable.TableIndexComparator reverseComparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", true))
        );

        assertTrue(reverseComparator.compare(0, 1) > 0);
        assertTrue(reverseComparator.compare(0, 2) == 0);
        assertTrue(reverseComparator.compare(1, 2) < 0);
    }

    public void testRowOutOfBounds() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();
        RestTable.TableIndexComparator comparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", false))
        );
        Error e = expectThrows(AssertionError.class, () -> { comparator.compare(0, 1); });
        assertEquals("Invalid comparison of indices (0, 1): Table has 0 rows.", e.getMessage());
    }

    public void testUnknownHeader() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();
        restRequest.params().put("s", "notaheader");
        Exception e = expectThrows(UnsupportedOperationException.class, () -> RestTable.getRowOrder(table, restRequest));
        assertEquals("Unable to sort by unknown sort key `notaheader`", e.getMessage());
    }

    public void testAliasSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare", "alias:c;");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(3, 1, 2);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "c");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(1, 2, 0), rowOrder);
    }

    public void testReversedSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("reversed");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(0, 1, 2);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "reversed:desc");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(2, 1, 0), rowOrder);
    }

    public void testMultiSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.addCell("second.compare");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(3, 3, 2);
        List<Integer> secondComparisonList = Arrays.asList(2, 1, 3);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.addCell(secondComparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "compare,second.compare");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(2, 1, 0), rowOrder);

        restRequest.params().put("s", "compare:desc,second.compare");
        rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(1, 0, 2), rowOrder);
    }

    public void testPlainTextChunking() throws Exception {
        final var cells = randomArray(8, 8, String[]::new, () -> randomAlphaOfLengthBetween(1, 5));
        final var expectedRow = String.join(" ", cells) + "\n";

        // OutputStreamWriter has an 8kiB buffer so all chunks are at least that big
        final var bufferSize = ByteSizeUnit.KB.toIntBytes(8);
        final var rowLength = expectedRow.length();
        final var expectedRowsPerChunk = 1 + bufferSize / rowLength; // end chunk after first row which overflows the buffer
        final var expectedChunkSize = expectedRowsPerChunk * rowLength;

        final var rowCount = between(expectedRowsPerChunk + 1, expectedRowsPerChunk * 10);
        final var expectedChunkCount = 1 + (rowCount * rowLength - 1) / expectedChunkSize; // ceil(rowCount * rowLength / expectedChunkSize)
        assertThat(expectedChunkCount, greaterThan(1));

        final var expectedBody = new StringBuilder();
        for (int i = 0; i < rowCount; i++) {
            table.startRow();
            for (final var cell : cells) {
                table.addCell(cell);
            }
            table.endRow();
            expectedBody.append(expectedRow);
        }

        final var request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Collections.singletonMap(ACCEPT, Collections.singletonList(TEXT_PLAIN))
        ).build();

        final var response = buildResponse(table, new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {}
        });

        final var bodyChunks = getBodyChunks(response, bufferSize);
        assertEquals("chunk count", expectedChunkCount, bodyChunks.size());
        assertEquals("first chunk size", expectedChunkSize, bodyChunks.get(0).length());
        assertEquals("body contents", expectedBody.toString(), String.join("", bodyChunks));
    }

    public void testEmptyTable() throws Exception {
        final var request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Collections.singletonMap(ACCEPT, Collections.singletonList(TEXT_PLAIN))
        ).build();

        final var response = buildResponse(table, new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {}
        });

        assertFalse(response.isChunked());
        assertThat(response.content().length(), equalTo(0));
    }

    public void testXContentChunking() throws Exception {
        final var jsonTableRow = JSON_TABLE_BODY.substring(1, JSON_TABLE_BODY.length() - 2).replaceAll("\\s+", "");
        final var expectedBody = new StringBuilder("[");
        final var rowCount = between(10000, 20000);
        for (int i = 0; i < rowCount; i++) {
            if (i != 0) {
                expectedBody.append(",");
            }
            table.startRow();
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.addCell("foo");
            table.endRow();
            expectedBody.append(jsonTableRow);
        }
        expectedBody.append("]");

        final var request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_JSON))
        ).build();

        final var response = buildResponse(table, new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {}
        });

        // layers of buffering make it hard to be precise here:
        final var bodyChunks = getBodyChunks(response, 8192);
        assertEquals(expectedBody.toString(), String.join("", bodyChunks));
        assertThat(bodyChunks.size(), greaterThan(1));
    }

    private RestResponse assertResponseContentType(Map<String, List<String>> headers, String mediaType) throws Exception {
        FakeRestRequest requestWithAcceptHeader = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();
        table.startRow();
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.endRow();
        RestResponse response = buildResponse(table, new AbstractRestChannel(requestWithAcceptHeader, true) {
            @Override
            public void sendResponse(RestResponse response) {}
        });
        String actualWithoutWhitespaces = mediaType.replaceAll("\\s+", "");
        String expectedWithoutWhitespaces = response.contentType().replaceAll("\\s+", "");
        assertThat(expectedWithoutWhitespaces, equalToIgnoringCase(actualWithoutWhitespaces));
        return response;
    }

    private void assertResponse(Map<String, List<String>> headers, String mediaType, String body) throws Exception {
        RestResponse response = assertResponseContentType(headers, mediaType);
        assertTrue(response.isChunked());
        assertThat(String.join("", getBodyChunks(response, between(1, 1024))), equalTo(body));
    }

    private static List<String> getBodyChunks(RestResponse response, final int pageSize) throws IOException {
        assertTrue(response.isChunked());

        final var openPages = new AtomicInteger();
        final var recycler = new Recycler<BytesRef>() {
            @Override
            public V<BytesRef> obtain() {
                openPages.incrementAndGet();
                return new V<>() {
                    final BytesRef page = new BytesRef(new byte[pageSize], 0, pageSize);

                    @Override
                    public BytesRef v() {
                        return page;
                    }

                    @Override
                    public boolean isRecycled() {
                        return false;
                    }

                    @Override
                    public void close() {
                        openPages.decrementAndGet();
                    }
                };
            }
        };

        final var bodyChunks = new ArrayList<String>();
        final var chunkedRestResponseBody = response.chunkedContent();

        while (chunkedRestResponseBody.isDone() == false) {
            try (var chunk = chunkedRestResponseBody.encodeChunk(pageSize, recycler)) {
                assertThat(chunk.length(), greaterThan(0));
                bodyChunks.add(chunk.utf8ToString());
            }
        }
        assertEquals(0, openPages.get());
        return bodyChunks;
    }

    private List<String> getHeaderNames(List<RestTable.DisplayHeader> headers) {
        List<String> headerNames = new ArrayList<>();
        for (RestTable.DisplayHeader header : headers) {
            headerNames.add(header.name);
        }

        return headerNames;
    }
}
