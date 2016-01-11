/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class PipelineStoreTests extends ESTestCase {

    private PipelineStore store;

    @Before
    public void init() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        store = new PipelineStore(Settings.EMPTY, clusterService);
        ProcessorsRegistry registry = new ProcessorsRegistry();
        registry.registerProcessor("set", (environment, templateService) -> new SetProcessor.Factory(TestTemplateService.instance()));
        store.buildProcessorFactoryRegistry(registry, null, null);
    }

    public void testUpdatePipelines() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        store.innerUpdatePipelines(clusterState);
        assertThat(store.pipelines.size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}")
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        store.innerUpdatePipelines(clusterState);
        assertThat(store.pipelines.size(), is(1));
        assertThat(store.pipelines.get("_id").getId(), equalTo("_id"));
        assertThat(store.pipelines.get("_id").getDescription(), nullValue());
        assertThat(store.pipelines.get("_id").getProcessors().size(), equalTo(1));
        assertThat(store.pipelines.get("_id").getProcessors().get(0).getType(), equalTo("set"));
    }

    public void testPut() {
        String id = "_id";
        Pipeline pipeline = store.get(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        // add a new pipeline:
        PutPipelineRequest putRequest = new PutPipelineRequest();
        putRequest.id(id);
        putRequest.source(new BytesArray("{\"processors\": []}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest = new PutPipelineRequest();
        putRequest.id(id);
        putRequest.source(new BytesArray("{\"processors\": [], \"description\": \"_description\"}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testDelete() {
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}")
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        store.innerUpdatePipelines(clusterState);
        assertThat(store.get("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest();
        deleteRequest.id("_id");
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        assertThat(store.get("_id"), nullValue());

        // Delete existing pipeline:
        try {
            store.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (PipelineMissingException e) {
            assertThat(e.getMessage(), equalTo("pipeline [_id] is missing"));
        }
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration(
            "_id1", new BytesArray("{\"processors\": []}")
        ));
        configs.put("_id2", new PipelineConfiguration(
            "_id2", new BytesArray("{\"processors\": []}")
        ));

        assertThat(store.innerGetPipelines(null, "_id1").isEmpty(), is(true));

        IngestMetadata ingestMetadata = new IngestMetadata(configs);
        List<PipelineConfiguration> pipelines = store.innerGetPipelines(ingestMetadata, "_id1");
        assertThat(pipelines.size(), equalTo(1));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));

        pipelines = store.innerGetPipelines(ingestMetadata, "_id1", "_id2");
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = store.innerGetPipelines(ingestMetadata, "_id*");
        pipelines.sort((o1, o2) -> o1.getId().compareTo(o2.getId()));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));
    }

    public void testCrud() throws Exception {
        String id = "_id";
        Pipeline pipeline = store.get(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest();
        putRequest.id(id);
        putRequest.source(new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest();
        deleteRequest.id(id);
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, nullValue());
    }

}
