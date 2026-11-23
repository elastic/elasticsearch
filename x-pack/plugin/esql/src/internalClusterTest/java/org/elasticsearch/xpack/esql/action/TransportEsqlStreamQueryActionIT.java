/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_STATUS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_PAGES;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_ROWS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class TransportEsqlStreamQueryActionIT extends AbstractEsqlIntegTestCase {

    private static final String SOURCE_INDEX = "stream_source";

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("ES|QL streaming is not available in release builds yet", Build.current().isSnapshot());
    }

    @Before
    public void enableBatchIndexing() {
        updateClusterSettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true));
    }

    @After
    public void resetBatchIndexing() {
        updateClusterSettings(Settings.builder().putNull(BatchIndexingEnabled.BATCH_INDEXING.getKey()));
    }

    public void testStoreNone() throws Exception {
        int docCount = indexDocs();

        List<String> lines = executeStream("FROM " + SOURCE_INDEX + " | LIMIT " + docCount, StreamOptions.Store.NONE);

        assertThat(lines, hasSize(docCount + 2));// + header and footer
        assertThat(parseJson(lines.getFirst()), not(hasKey(STREAM_ID_FIELD)));
        assertFalse("nothing must be stored, so the results index must not be created", indexExists(INDEX));
    }

    public void testStoreAll() throws Exception {
        int docCount = indexDocs();

        List<String> lines = executeStream("FROM " + SOURCE_INDEX + " | LIMIT " + docCount, StreamOptions.Store.ALL);

        assertThat(lines, hasSize(docCount + 2));// + header and footer
        String streamId = (String) parseJson(lines.getFirst()).get(STREAM_ID_FIELD);
        assertNotNull("the first line must carry the stream id", streamId);

        assertBusy(() -> assertThat(getMetadataDocument(streamId).get(STREAM_STATUS), equalTo("COMPLETE")));

        Map<String, Object> metadata = getMetadataDocument(streamId);
        assertThat(((Number) metadata.get(STREAM_TOTAL_ROWS)).longValue(), equalTo((long) docCount));
        int pages = ((Number) metadata.get(STREAM_TOTAL_PAGES)).intValue();
        assertThat(pages, greaterThan(0));

        List<Map<String, Object>> stored = getPageDocuments(streamId);
        assertThat(stored, hasSize(pages));
        long offset = 0;
        for (int seq = 0; seq < pages; seq++) {
            Map<String, Object> page = stored.get(seq);
            assertThat(((Number) page.get(PAGE_SEQ_FIELD)).intValue(), equalTo(seq));
            assertThat(((Number) page.get(PAGE_OFFSET_FIELD)).longValue(), equalTo(offset));
            assertNotNull(page.get(PAGE_FIELD));
            offset += ((Number) page.get(PAGE_SIZE_FIELD)).intValue();
        }
        assertThat(offset, equalTo((long) docCount));
    }

    public void testStoreOverflow() throws Exception {
        int docCount = indexDocs();
        int maxRows = randomIntBetween(1, docCount - 1);

        List<String> lines = executeStream(
            "FROM " + SOURCE_INDEX + " | LIMIT " + docCount,
            new StreamOptions(StreamOptions.Store.OVERFLOW, maxRows, StreamOptions.NO_LIMIT)
        );

        assertThat(lines, hasSize(maxRows + 2));// + header and footer
        String streamId = (String) parseJson(lines.getFirst()).get(STREAM_ID_FIELD);
        assertNotNull("the first line must carry the stream id", streamId);

        assertBusy(() -> assertThat(getMetadataDocument(streamId).get(STREAM_STATUS), equalTo("COMPLETE")));

        Map<String, Object> metadata = getMetadataDocument(streamId);
        assertThat(((Number) metadata.get(STREAM_TOTAL_ROWS)).longValue(), equalTo((long) docCount));
        int pages = ((Number) metadata.get(STREAM_TOTAL_PAGES)).intValue();

        List<Map<String, Object>> stored = getPageDocuments(streamId);
        assertThat(stored, not(empty()));

        int firstSeq = ((Number) stored.getFirst().get(PAGE_SEQ_FIELD)).intValue();
        long firstOffset = ((Number) stored.getFirst().get(PAGE_OFFSET_FIELD)).longValue();
        long firstSize = ((Number) stored.getFirst().get(PAGE_SIZE_FIELD)).longValue();
        assertThat("the first stored page must hold the first unsent row", firstOffset, lessThanOrEqualTo((long) maxRows));
        assertThat("the first stored page must hold the first unsent row", firstOffset + firstSize, greaterThan((long) maxRows));
        assertThat("pages are stored up to the last one", firstSeq + stored.size(), equalTo(pages));
        long offset = firstOffset;
        for (int i = 0; i < stored.size(); i++) {
            Map<String, Object> page = stored.get(i);
            assertThat(((Number) page.get(PAGE_SEQ_FIELD)).intValue(), equalTo(firstSeq + i));
            assertThat(((Number) page.get(PAGE_OFFSET_FIELD)).longValue(), equalTo(offset));
            assertNotNull(page.get(PAGE_FIELD));
            offset += ((Number) page.get(PAGE_SIZE_FIELD)).intValue();
        }
        assertThat("every row after the sent ones must be stored", offset, equalTo((long) docCount));
    }

    public void testStoreOverflowWhenResultFitsTheLimit() throws Exception {
        int docCount = indexDocs();
        int maxRows = randomIntBetween(docCount, docCount * 2);

        List<String> lines = executeStream(
            "FROM " + SOURCE_INDEX + " | LIMIT " + docCount,
            new StreamOptions(StreamOptions.Store.OVERFLOW, maxRows, StreamOptions.NO_LIMIT)
        );

        assertThat(lines, hasSize(docCount + 2));// + header and footer
        Map<String, Object> footer = parseJson(lines.getLast());
        assertThat(footer.get("complete"), equalTo(true));
        assertThat(footer.get("next_offset"), equalTo(docCount));
        String streamId = (String) parseJson(lines.getFirst()).get(STREAM_ID_FIELD);
        assertNotNull("the first line must carry the stream id", streamId);

        assertBusy(() -> assertThat(getMetadataDocument(streamId).get(STREAM_STATUS), equalTo("COMPLETE")));

        Map<String, Object> metadata = getMetadataDocument(streamId);
        assertThat(((Number) metadata.get(STREAM_TOTAL_ROWS)).longValue(), equalTo((long) docCount));
        assertThat("every page was sent, so none overflowed", getPageDocuments(streamId), empty());
    }

    public void testStoreWhenQueryReturnsNoRows() throws Exception {
        indexDocs();
        StreamOptions.Store store = randomFrom(StreamOptions.Store.OVERFLOW, StreamOptions.Store.ALL);

        List<String> lines = executeStream("FROM " + SOURCE_INDEX + " | WHERE value < 0", store);

        assertThat(lines, hasSize(2));// header and footer
        Map<String, Object> footer = parseJson(lines.getLast());
        assertThat(footer.get("complete"), equalTo(true));
        assertThat(footer.get("next_offset"), equalTo(0));
        String streamId = (String) parseJson(lines.getFirst()).get(STREAM_ID_FIELD);
        assertNotNull("the first line must carry the stream id", streamId);

        assertBusy(() -> assertThat(getMetadataDocument(streamId).get(STREAM_STATUS), equalTo("COMPLETE")));

        Map<String, Object> metadata = getMetadataDocument(streamId);
        assertThat(((Number) metadata.get(STREAM_TOTAL_ROWS)).longValue(), equalTo(0L));
        assertThat(((Number) metadata.get(STREAM_TOTAL_PAGES)).longValue(), equalTo(0L));
        assertThat("there is nothing to store", getPageDocuments(streamId), empty());
    }

    private static Map<String, Object> parseJson(String header) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, header, false);
    }

    private int indexDocs() {
        int docCount = between(100, 500);
        assertAcked(prepareCreate(SOURCE_INDEX).setMapping("value", "type=integer"));
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex(SOURCE_INDEX).setSource(Map.of("value", i)));
        }
        indexRandom(true, docs);
        return docCount;
    }

    private List<String> executeStream(String query, StreamOptions.Store store) throws IOException {
        return executeStream(query, new StreamOptions(store, StreamOptions.NO_LIMIT, StreamOptions.NO_LIMIT));
    }

    private List<String> executeStream(String query, StreamOptions options) throws IOException {
        PlainActionFuture<RestResponse> responseFuture = new PlainActionFuture<>();
        var channel = new AbstractRestChannel(new FakeRestRequest(), true) {
            @Override
            public void sendResponse(RestResponse response) {
                responseFuture.onResponse(response);
            }
        };
        var listener = new EsqlStreamResponseListener(
            channel,
            client().threadPool().getThreadContext(),
            BigArrays.NON_RECYCLING_INSTANCE.bytesRefRecycler(),
            client(),
            options
        );
        client().execute(
            EsqlStreamQueryAction.INSTANCE,
            new EsqlStreamQueryRequest(
                EsqlQueryRequest.syncEsqlQueryRequest(query),
                listener.resultStreamListener(),
                false,
                listener::setPreHeaderFailureFooter
            ),
            listener
        );
        try (RestResponse response = safeGet(responseFuture)) {
            return drain(response);
        }
    }

    private static List<String> drain(RestResponse response) throws IOException {
        List<String> lines = new ArrayList<>();
        ChunkedRestResponseBodyPart part = response.chunkedContent();
        while (true) {
            while (part.isPartComplete() == false) {
                lines.addAll(part.encodeChunk(1024, BytesRefRecycler.NON_RECYCLING_INSTANCE).utf8ToString().lines().toList());
            }
            if (part.isLastPart()) {
                return lines;
            }
            PlainActionFuture<ChunkedRestResponseBodyPart> next = new PlainActionFuture<>();
            part.getNextPart(next);
            part = safeGet(next);
        }
    }

    private Map<String, Object> getMetadataDocument(String id) {
        GetResponse response = client().prepareGet(INDEX, id).setRouting(id).get();
        assertTrue("document [" + id + "] must exist", response.isExists());
        return response.getSourceAsMap();
    }

    /** The page documents of the stream in page order, without its metadata document. */
    private List<Map<String, Object>> getPageDocuments(String streamId) {
        indicesAdmin().prepareRefresh(INDEX).get();
        List<Map<String, Object>> pages = new ArrayList<>();
        assertResponse(
            prepareSearch(INDEX).setQuery(termQuery(STREAM_ID_FIELD, streamId)).addSort(PAGE_SEQ_FIELD, SortOrder.ASC).setSize(10_000),
            response -> {
                for (SearchHit hit : response.getHits()) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    if (((Number) source.get(PAGE_SEQ_FIELD)).intValue() >= 0) {
                        pages.add(source);
                    }
                }
            }
        );
        return pages;
    }
}
