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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
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
                    return tag;
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
                    return tag;
                }
            };
        });
        store = new PipelineStore(Settings.EMPTY, processorFactories);
    }

    public void testUpdatePipelines() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.pipelines.size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        store.innerUpdatePipelines(previousClusterState, clusterState);
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
        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
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

        PutPipelineRequest putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"description\": \"empty processors\"}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = store.innerPut(putRequest, clusterState);
        try {
            store.innerUpdatePipelines(previousClusterState, clusterState);
            fail("should fail");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
        pipeline = store.get(id);
        assertThat(pipeline, nullValue());
    }

    public void testDelete() {
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        previousClusterState = clusterState;
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("_id"), nullValue());

        // Delete existing pipeline:
        try {
            store.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [_id] is missing"));
        }
    }

    public void testDeleteUsingWildcard() {
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        pipelines.put("p2", new PipelineConfiguration("p2", definition, XContentType.JSON));
        pipelines.put("q1", new PipelineConfiguration("q1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("p1"), notNullValue());
        assertThat(store.get("p2"), notNullValue());
        assertThat(store.get("q1"), notNullValue());

        // Delete pipeline matching wildcard
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("p*");
        previousClusterState = clusterState;
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("p1"), nullValue());
        assertThat(store.get("p2"), nullValue());
        assertThat(store.get("q1"), notNullValue());

        // Exception if we used name which does not exist
        try {
            store.innerDelete(new DeletePipelineRequest("unknown"), clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [unknown] is missing"));
        }

        // match all wildcard works on last remaining pipeline
        DeletePipelineRequest matchAllDeleteRequest = new DeletePipelineRequest("*");
        previousClusterState = clusterState;
        clusterState = store.innerDelete(matchAllDeleteRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("p1"), nullValue());
        assertThat(store.get("p2"), nullValue());
        assertThat(store.get("q1"), nullValue());

        // match all wildcard does not throw exception if none match
        store.innerDelete(matchAllDeleteRequest, clusterState);
    }

    public void testDeleteWithExistingUnmatchedPipelines() {
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        store.innerUpdatePipelines(previousClusterState, clusterState);
        assertThat(store.get("p1"), notNullValue());

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("z*");
        try {
            store.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [z*] is missing"));
        }
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration(
            "_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON
        ));
        configs.put("_id2", new PipelineConfiguration(
            "_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON
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
                new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = store.innerPut(putRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        previousClusterState = clusterState;
        clusterState = store.innerDelete(deleteRequest, clusterState);
        store.innerUpdatePipelines(previousClusterState, clusterState);
        pipeline = store.get(id);
        assertThat(pipeline, nullValue());
    }

    public void testValidate() throws Exception {
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}}," +
                    "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"),
            XContentType.JSON);

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", buildNewFakeTransportAddress(),
                emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", buildNewFakeTransportAddress(),
                emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        ElasticsearchParseException e =
            expectThrows(ElasticsearchParseException.class, () -> store.validatePipeline(ingestInfos, putRequest));
        assertEquals("Processor type [remove] is not installed on node [" + node2 + "]", e.getMessage());
        assertEquals("remove", e.getHeader("processor_type").get(0));
        assertEquals("tag2", e.getHeader("processor_tag").get(0));

        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        store.validatePipeline(ingestInfos, putRequest);
    }

    public void testValidateNoIngestInfo() throws Exception {
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        Exception e = expectThrows(IllegalStateException.class, () -> store.validatePipeline(Collections.emptyMap(), putRequest));
        assertEquals("Ingest info is empty", e.getMessage());

        DiscoveryNode discoveryNode = new DiscoveryNode("_node_id", buildNewFakeTransportAddress(),
                emptyMap(), emptySet(), Version.CURRENT);
        IngestInfo ingestInfo = new IngestInfo(Collections.singletonList(new ProcessorInfo("set")));
        store.validatePipeline(Collections.singletonMap(discoveryNode, ingestInfo), putRequest);
    }
}
