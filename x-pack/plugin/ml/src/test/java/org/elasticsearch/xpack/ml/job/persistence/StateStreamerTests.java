/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StateStreamerTests extends ESTestCase {

    private static final String CLUSTER_NAME = "state_streamer_cluster";
    private static final String JOB_ID = "state_streamer_test_job";

    public void testRestoreStateToStream() throws Exception {
        String snapshotId = "123";
        Map<String, Object> categorizerState = new HashMap<>();
        categorizerState.put("catName", "catVal");
        GetResponse categorizerStateGetResponse1 = createGetResponse(true, categorizerState);
        GetResponse categorizerStateGetResponse2 = createGetResponse(false, null);
        Map<String, Object> modelState = new HashMap<>();
        modelState.put("modName", "modVal1");
        GetResponse modelStateGetResponse1 = createGetResponse(true, modelState);
        modelState.put("modName", "modVal2");
        GetResponse modelStateGetResponse2 = createGetResponse(true, modelState);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE,
                        CategorizerState.documentId(JOB_ID, 1), categorizerStateGetResponse1)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE,
                        CategorizerState.documentId(JOB_ID, 2), categorizerStateGetResponse2)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE,
                        ModelState.documentId(JOB_ID, snapshotId, 1), modelStateGetResponse1)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE,
                        ModelState.documentId(JOB_ID, snapshotId, 2), modelStateGetResponse2);


        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID).setSnapshotId(snapshotId).setSnapshotDocCount(2).build();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        StateStreamer stateStreamer = new StateStreamer(clientBuilder.build());
        stateStreamer.restoreStateToStream(JOB_ID, modelSnapshot, stream);

        String[] restoreData = stream.toString(StandardCharsets.UTF_8.name()).split("\0");
        assertEquals(3, restoreData.length);
        assertEquals("{\"modName\":\"modVal1\"}", restoreData[0]);
        assertEquals("{\"modName\":\"modVal2\"}", restoreData[1]);
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

    private static GetResponse createGetResponse(boolean exists, Map<String, Object> source) throws IOException {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(exists);
        when(getResponse.getSourceAsBytesRef()).thenReturn(BytesReference.bytes(XContentFactory.jsonBuilder().map(source)));
        return getResponse;
    }
}