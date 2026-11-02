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
import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.datastreams.DataStreamIT.putComposableIndexTemplate;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Renaming a backing index of a data stream must keep the data stream pointing at the same index under its new name. This is
 * the scenario ILM hits when it shrinks or snapshots a backing index and wants the result to carry the original name.
 */
public class DataStreamRenameIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testRenameBackingIndex() throws Exception {
        final String dataStreamName = "metrics-foo";
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).get();
        client().prepareIndex(dataStreamName)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("{\"@timestamp\": \"2020-12-12\", \"field\": \"value\"}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ElasticsearchAssertions.assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get());

        DataStream dataStream = getDataStream(dataStreamName);
        assertThat(dataStream.getIndices().size(), equalTo(2));
        final String firstBackingIndex = dataStream.getIndices().get(0).getName();
        final String firstBackingIndexUuid = dataStream.getIndices().get(0).getUUID();
        final String writeIndex = dataStream.getWriteIndex().getName();
        final long generation = dataStream.getGeneration();
        final String renamed = "metrics-foo-renamed-000001";

        // The write index cannot be renamed
        var e = expectThrows(IllegalArgumentException.class, () -> rename(writeIndex, "metrics-foo-write-renamed"));
        assertThat(e.getMessage(), containsString("write index"));

        rename(firstBackingIndex, renamed);

        dataStream = getDataStream(dataStreamName);
        assertThat(dataStream.getIndices().size(), equalTo(2));
        assertThat(dataStream.getIndices().get(0).getName(), equalTo(renamed));
        assertThat(dataStream.getIndices().get(0).getUUID(), equalTo(firstBackingIndexUuid));
        assertThat(dataStream.getWriteIndex().getName(), equalTo(writeIndex));
        // TODO: the prototype bumps the generation because it reuses DataStream#replaceBackingIndex; a rename should not do that
        assertThat(dataStream.getGeneration(), equalTo(generation + 1));

        // The document is still reachable through the data stream and reports the new index name
        ElasticsearchAssertions.assertResponse(client().prepareSearch(dataStreamName), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(response.getHits().getAt(0).getIndex(), equalTo(renamed));
        });
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed), 1L);
    }

    private void rename(String source, String target) {
        var response = client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, source, target)
        ).actionGet();
        assertTrue(response.isAcknowledged());
    }

    private DataStream getDataStream(String name) {
        var response = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { name })
        ).actionGet();
        assertThat(response.getDataStreams().size(), equalTo(1));
        return response.getDataStreams().get(0).getDataStream();
    }
}
