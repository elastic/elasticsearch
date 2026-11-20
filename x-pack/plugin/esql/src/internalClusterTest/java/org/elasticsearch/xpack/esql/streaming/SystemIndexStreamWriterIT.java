/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.junit.After;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_TRANSPORT_VERSION_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class SystemIndexStreamWriterIT extends AbstractEsqlIntegTestCase {

    @After
    public void resetBatchIndexing() {
        updateClusterSettings(Settings.builder().putNull(BatchIndexingEnabled.BATCH_INDEXING.getKey()));
    }

    public void testStartAppendPagesAndComplete() throws Exception {
        updateClusterSettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true));

        TransportVersion transportVersion = TransportVersion.current();
        var writer = new SystemIndexStreamWriter(client(), BytesRefRecycler.NON_RECYCLING_INSTANCE, transportVersion);
        TaskId taskId = new TaskId(randomIdentifier(), randomNonNegativeLong());
        Instant expiration = Instant.ofEpochMilli(System.currentTimeMillis()).plusSeconds(randomIntBetween(30, 3600));
        List<ColumnInfo> columns = List.of(new ColumnInfoImpl("value", "integer", null));
        Map<String, String> headers = Map.of(randomIdentifier(), randomIdentifier());
        List<String> warnings = List.of(randomAlphaOfLengthBetween(5, 30));

        String streamId = writer.createStarted(taskId, expiration, columns, headers, warnings);
        var capacity = new CountDownLatch(1);
        writer.onCapacity(capacity::countDown);
        safeAwait(capacity);

        PageSpec[] pages = new PageSpec[randomIntBetween(16, 32)];
        long rows = 0;
        for (int i = 0; i < pages.length; i++) {
            int positions = randomIntBetween(1, 5);
            pages[i] = new PageSpec(i + 1, rows, positions);
            rows += positions;
        }
        var complete = new CountDownLatch(1);
        appendPages(writer, pages, 0, rows, complete);
        safeAwait(complete);

        var response = client().prepareGet(INDEX, streamId).setRouting(streamId).get();
        assertTrue(response.isExists());
        assertThat(
            StreamMetadataDocument.fromSource(response.getSourceAsMap()),
            equalTo(
                StreamMetadata.started(streamId, taskId, expiration, columns, headers, warnings).complete(false, 10, pages.length, rows)
            )
        );
        for (PageSpec page : pages) {
            assertPage(streamId, page, transportVersion);
        }
    }

    public void testFinalStatusIsNotOverwritten() throws Exception {
        updateClusterSettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true));

        var writer = new SystemIndexStreamWriter(client(), BytesRefRecycler.NON_RECYCLING_INSTANCE, TransportVersion.current());
        TaskId taskId = new TaskId(randomIdentifier(), randomNonNegativeLong());
        Instant expiration = Instant.ofEpochMilli(System.currentTimeMillis()).plusSeconds(randomIntBetween(30, 3600));
        List<ColumnInfo> columns = List.of(new ColumnInfoImpl("value", "integer", null));
        Map<String, String> headers = Map.of(randomIdentifier(), randomIdentifier());
        List<String> warnings = List.of(randomAlphaOfLengthBetween(5, 30));

        String streamId = writer.createStarted(taskId, expiration, columns, headers, warnings);
        awaitCapacity(writer);
        writer.markComplete(false, 10, 0, 0);
        awaitCapacity(writer);

        writer.markFailed(false, 20, new RuntimeException("test"));
        writer.markCanceled();
        try (Page page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantIntBlockWith(1, 3))) {
            writer.appendPage(1, 0, page);
        }
        awaitCapacity(writer);

        StreamMetadata expected = StreamMetadata.started(streamId, taskId, expiration, columns, headers, warnings)
            .complete(false, 10, 0, 0);
        var response = client().prepareGet(INDEX, streamId).setRouting(streamId).get();
        assertTrue(response.isExists());
        assertThat(StreamMetadataDocument.fromSource(response.getSourceAsMap()), equalTo(expected));
        assertFalse(client().prepareGet(INDEX, streamId + "_1").setRouting(streamId).get().isExists());
    }

    private static void awaitCapacity(SystemIndexStreamWriter writer) {
        var capacity = new CountDownLatch(1);
        writer.onCapacity(capacity::countDown);
        safeAwait(capacity);
    }

    private record PageSpec(int seq, long offset, int positions) {}

    private static void appendPages(SystemIndexStreamWriter writer, PageSpec[] pages, int index, long rows, CountDownLatch complete) {
        writer.onCapacity(() -> {
            var p = pages[index];
            try (Page page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantIntBlockWith(p.seq(), p.positions()))) {
                writer.appendPage(p.seq(), p.offset(), page);
            }
            if (index + 1 < pages.length) {
                appendPages(writer, pages, index + 1, rows, complete);
            } else {
                writer.markComplete(false, 10, pages.length, rows);
                writer.onCapacity(complete::countDown);
            }
        });
    }

    private void assertPage(String streamId, PageSpec spec, TransportVersion transportVersion) {
        var response = client().prepareGet(INDEX, streamId + "_" + spec.seq()).setRouting(streamId).get();
        assertTrue(response.isExists());
        Map<String, Object> source = response.getSourceAsMap();
        assertThat(((Number) source.get(PAGE_SEQ_FIELD)).intValue(), equalTo(spec.seq()));
        assertThat(((Number) source.get(PAGE_OFFSET_FIELD)).longValue(), equalTo(spec.offset()));
        assertThat(((Number) source.get(PAGE_SIZE_FIELD)).intValue(), equalTo(spec.positions()));
        assertThat(((Number) source.get(PAGE_TRANSPORT_VERSION_FIELD)).intValue(), equalTo(transportVersion.id()));
        assertNotNull(source.get(PAGE_FIELD));
    }
}
