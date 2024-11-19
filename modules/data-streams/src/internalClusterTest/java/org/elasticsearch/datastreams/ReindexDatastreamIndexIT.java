/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.ReindexDataStreamIndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.datastreams.action.TransportReindexDataStreamIndexAction;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDatastreamIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testDestIndexDeletedIfExists() throws Exception {
        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // dest index with docs
        var destIndex = TransportReindexDataStreamIndexAction.generateDestIndexName(sourceIndex);
        indicesAdmin().create(new CreateIndexRequest(destIndex)).actionGet();
        indexDocs(destIndex, 10);
        assertHitCount(prepareSearch(destIndex).setSize(0), 10);

        // call reindex
        var response = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet();

        // verify that dest still exists, but is now empty
        assertTrue(indexExists(destIndex));
        assertHitCount(prepareSearch(destIndex).setSize(0), 0);
    }

    public void testDestIndexContainsDocs() throws Exception {
        // empty source index
        var numDocs = randomIntBetween(1, 100);
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();
        indexDocs(sourceIndex, numDocs);

        // dest index with docs
        var expectedDestIndexName = TransportReindexDataStreamIndexAction.generateDestIndexName(sourceIndex);

        // call reindex
        var response = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet();
        indicesAdmin().refresh(new RefreshRequest(response.getDestIndex())).actionGet();

        // verify that dest contains docs
        assertEquals(expectedDestIndexName, response.getDestIndex());
        assertHitCount(prepareSearch(response.getDestIndex()).setSize(0), numDocs);
    }

    static void indexDocs(String index, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        indicesAdmin().refresh(new RefreshRequest(index)).actionGet();
    }

}
