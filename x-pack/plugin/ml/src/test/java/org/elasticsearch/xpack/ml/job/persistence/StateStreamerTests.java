/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StateStreamerTests extends ESTestCase {

    private static final String CLUSTER_NAME = "state_streamer_cluster";
    private static final String JOB_ID = "state_streamer_test_job";

    public void testRestoreStateToStream() throws Exception {
        String snapshotId = "123";
        Map<String, Object> categorizerState = new HashMap<>();
        categorizerState.put("catName", "catVal");
        Map<String, Object> modelState1 = new HashMap<>();
        modelState1.put("modName1", "modVal1");
        Map<String, Object> modelState2 = new HashMap<>();
        modelState2.put("modName2", "modVal2");


        SearchRequestBuilder builder1 = prepareSearchBuilder(createSearchResponse(Collections.singletonList(modelState1)),
            QueryBuilders.idsQuery().addIds(ModelState.documentId(JOB_ID, snapshotId, 1)));
        SearchRequestBuilder builder2 = prepareSearchBuilder(createSearchResponse(Collections.singletonList(modelState2)),
            QueryBuilders.idsQuery().addIds(ModelState.documentId(JOB_ID, snapshotId, 2)));
        SearchRequestBuilder builder3 = prepareSearchBuilder(createSearchResponse(Collections.singletonList(categorizerState)),
            QueryBuilders.idsQuery().addIds(CategorizerState.documentId(JOB_ID, 1)));
        SearchRequestBuilder builder4 = prepareSearchBuilder(createSearchResponse(Collections.emptyList()),
            QueryBuilders.idsQuery().addIds(CategorizerState.documentId(JOB_ID, 2)));

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
            .addClusterStatusYellowResponse()
            .prepareSearches(AnomalyDetectorsIndex.jobStateIndexPattern(), builder1, builder2, builder3, builder4);

        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID).setSnapshotId(snapshotId).setSnapshotDocCount(2).build();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        StateStreamer stateStreamer = new StateStreamer(clientBuilder.build());
        stateStreamer.restoreStateToStream(JOB_ID, modelSnapshot, stream);

        String[] restoreData = stream.toString(StandardCharsets.UTF_8.name()).split("\0");
        assertEquals(3, restoreData.length);
        assertEquals("{\"modName1\":\"modVal1\"}", restoreData[0]);
        assertEquals("{\"modName2\":\"modVal2\"}", restoreData[1]);
        assertEquals("{\"catName\":\"catVal\"}", restoreData[2]);
    }

    public void testCancelBeforeRestoreWasCalled() throws IOException {
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID).setSnapshotId("snapshot_id").setSnapshotDocCount(2).build();
        OutputStream outputStream = mock(OutputStream.class);
        StateStreamer stateStreamer = new StateStreamer(mock(Client.class));
        stateStreamer.cancel();

        stateStreamer.restoreStateToStream(JOB_ID, modelSnapshot, outputStream);

        Mockito.verifyNoMoreInteractions(outputStream);
    }

    private static SearchResponse createSearchResponse(List<Map<String, Object>> source) throws IOException {
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHit[] hits = new SearchHit[source.size()];
        int i = 0;
        for (Map<String, Object> s : source) {
            SearchHit hit = new SearchHit(1).sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(s)));
            hits[i++] = hit;
        }
        SearchHits searchHits = new SearchHits(hits, null, (float)0.0);
        when(searchResponse.getHits()).thenReturn(searchHits);
        return searchResponse;
    }

    private static SearchRequestBuilder prepareSearchBuilder(SearchResponse response, QueryBuilder queryBuilder) {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.addSort(any(SortBuilder.class))).thenReturn(builder);
        when(builder.setQuery(queryBuilder)).thenReturn(builder);
        when(builder.setPostFilter(any())).thenReturn(builder);
        when(builder.setFrom(anyInt())).thenReturn(builder);
        when(builder.setSize(anyInt())).thenReturn(builder);
        when(builder.setFetchSource(eq(true))).thenReturn(builder);
        when(builder.addDocValueField(any(String.class))).thenReturn(builder);
        when(builder.addDocValueField(any(String.class), any(String.class))).thenReturn(builder);
        when(builder.addSort(any(String.class), any(SortOrder.class))).thenReturn(builder);
        when(builder.get()).thenReturn(response);
        return builder;
    }
}
