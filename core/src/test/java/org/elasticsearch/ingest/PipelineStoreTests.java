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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PipelineStoreTests extends ESTestCase {

    private PipelineStore store;

    @Before
    public void init() throws Exception {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        processorFactories.put("set", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");
            return new Processor() {
                @Override
                public void execute(IngestDocument ingestDocument) throws Exception {
                    ingestDocument.setFieldValue(field, value);
                }

                @Override
                public String getType() {
                    return "set";
                }

                @Override
                public String getTag() {
                    return null;
                }
            };
        });
        processorFactories.put("remove", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            return new Processor() {
                @Override
                public void execute(IngestDocument ingestDocument) throws Exception {
                    ingestDocument.removeField(field);
                }

                @Override
                public String getType() {
                    return "remove";
                }

                @Override
                public String getTag() {
                    return null;
                }
            };
        });
        store = new PipelineStore(Settings.EMPTY, processorFactories);
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
        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": []}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": [], \"description\": \"_description\"}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testPutWithErrorResponse() {
        String id = "_id";
        Pipeline pipeline = store.get(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"description\": \"empty processors\"}"));
        clusterState = store.innerPut(putRequest, clusterState);
        try {
            store.innerUpdatePipelines(clusterState);
            fail("should fail");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
        pipeline = store.get(id);
        assertThat(pipeline, nullValue());
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
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        assertThat(store.get("_id"), nullValue());

        // Delete existing pipeline:
        try {
            store.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
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

        // get all variants: (no IDs or '*')
        pipelines = store.innerGetPipelines(ingestMetadata);
        pipelines.sort((o1, o2) -> o1.getId().compareTo(o2.getId()));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = store.innerGetPipelines(ingestMetadata, "*");
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

        PutPipelineRequest putRequest = new PutPipelineRequest(id,
                new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"));
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, nullValue());
    }

    public void testValidate() throws Exception {
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}},{\"remove\" : {\"field\": \"_field\"}}]}"));

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", new LocalTransportAddress("_id"),
                emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", new LocalTransportAddress("_id"),
                emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        try {
            store.validatePipeline(ingestInfos, putRequest);
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Processor type [remove] is not installed on node [" + node2 + "]"));
        }

        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        store.validatePipeline(ingestInfos, putRequest);
    }

    public void testValidateNoIngestInfo() throws Exception {
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"));
        try {
            store.validatePipeline(Collections.emptyMap(), putRequest);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("Ingest info is empty"));
        }

        DiscoveryNode discoveryNode = new DiscoveryNode("_node_id", new LocalTransportAddress("_id"),
                emptyMap(), emptySet(), Version.CURRENT);
        IngestInfo ingestInfo = new IngestInfo(Collections.singletonList(new ProcessorInfo("set")));
        store.validatePipeline(Collections.singletonMap(discoveryNode, ingestInfo), putRequest);
    }

}
