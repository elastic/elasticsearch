/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * A writer on a search node coordinates its bulks there and sends them to the index nodes.
 */
public class StatelessEscfBulkWriterIT extends AbstractStatelessPluginIntegTestCase {

    public void testWriteFromASearchNode() throws Exception {
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
        Settings batchIndexing = Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build();
        startMasterOnlyNode(batchIndexing);
        startIndexNode(batchIndexing);
        String searchNode = startSearchNode(batchIndexing);
        ensureStableCluster(3);

        String index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indicesAdmin().prepareCreate(index)
            .setSettings(
                indexSettings(1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            )
            .setMapping("""
                {"dynamic":"strict","properties":{"name":{"type":"keyword"},"value":{"type":"long"}}}
                """)
            .get();
        ensureGreen(index);

        EscfBulkService service = internalCluster().getInstance(EscfBulkService.class, searchNode);
        CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class, searchNode).getBreaker(CircuitBreaker.REQUEST);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, searchNode);
        long breakerBefore = breaker.getUsed();
        long opsBefore = indexingPressure.stats().getTotalCoordinatingOps();
        long bytesBefore = indexingPressure.stats().getCurrentCoordinatingBytes();

        int producers = between(1, 4);
        EscfBulkWriter writer = service.newWriter(
            internalCluster().client(searchNode),
            breaker,
            index,
            new EscfBulkWriter.Policy(between(1, producers), ByteSizeValue.ofKb(between(1, 64)).getBytes()),
            EscfBulkWriter.ResponseHandler.FAIL_ON_FAILED_ITEM,
            () -> {}
        );
        int pages = between(1, 5);
        int rowsPerPage = between(1, 100);
        List<EscfBulkWriter.Producer> handles = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            handles.add(writer.newProducer());
        }
        // the producers add in turn, each its own rows
        for (int i = 0; i < pages; i++) {
            for (int p = 0; p < producers; p++) {
                handles.get(p).add(rows(index, (p * pages + i) * rowsPerPage, rowsPerPage), false);
            }
        }
        handles.forEach(EscfBulkWriter.Producer::close);
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        assertNull(safeGet(done));
        long rows = (long) producers * pages * rowsPerPage;

        // the search node counted each row once, and gave back what it counted and charged
        assertThat(indexingPressure.stats().getTotalCoordinatingOps() - opsBefore, equalTo(rows));
        assertThat(indexingPressure.stats().getCurrentCoordinatingBytes(), equalTo(bytesBefore));
        assertThat(breaker.getUsed(), equalTo(breakerBefore));

        indicesAdmin().prepareRefresh(index).get();
        assertHitCount(prepareSearch(index).setSize(0).setTrackTotalHits(true), rows);
        for (int i = 0; i < 5; i++) {
            int n = between(0, Math.toIntExact(rows) - 1);
            assertResponse(prepareSearch(index).setQuery(QueryBuilders.idsQuery().addIds("doc-" + n)), response -> {
                assertHitCount(response, 1);
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.get("name"), equalTo("name-" + n));
                assertThat(((Number) source.get("value")).longValue(), equalTo((long) n));
            });
        }
    }

    private static EscfBulkWriter.Rows rows(String index, int first, int count) {
        return new EscfBulkWriter.Rows() {
            @Override
            public int count() {
                return count;
            }

            @Override
            public void append(EscfBatchBuilder batch) {
                for (int i = 0; i < count; i++) {
                    byte[] name = ("name-" + (first + i)).getBytes(StandardCharsets.UTF_8);
                    EscfRowBuffer row = batch.beginRow();
                    row.stringField("name", new XContentString.UTF8Bytes(name, 0, name.length));
                    row.longField("value", first + i);
                    row.finishRow();
                    batch.commit(0);
                }
            }

            @Override
            public IndexRequest request(int row) {
                return new IndexRequest(index).id("doc-" + (first + row)).opType(DocWriteRequest.OpType.CREATE);
            }
        };
    }
}
