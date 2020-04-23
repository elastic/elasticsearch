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
package org.elasticsearch.action.admin.indices.datastream;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction.Request;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteDataStreamRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(8));
    }

    public void testValidateRequest() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request("my-data-stream");
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutName() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request("");
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("name is missing"));
    }

    public void testDeleteDataStream() {
        final String dataStreamName = "my-data-stream";
        final List<String> otherIndices = randomSubsetOf(List.of("foo", "bar", "baz"));

        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), otherIndices);
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(dataStreamName);
        ClusterState newState = DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(0));
        assertThat(newState.metadata().indices().size(), equalTo(otherIndices.size()));
        for (String indexName : otherIndices) {
            assertThat(newState.metadata().indices().get(indexName).getIndex().getName(), equalTo(indexName));
        }
    }

    public void testDeleteNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(dataStreamName);
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
            () -> DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req));
        assertThat(e.getMessage(), containsString("data_streams matching [" + dataStreamName + "] not found"));
    }

    @SuppressWarnings("unchecked")
    private static MetadataDeleteIndexService getMetadataDeleteIndexService() {
        MetadataDeleteIndexService s = mock(MetadataDeleteIndexService.class);
        when(s.deleteIndices(any(ClusterState.class), any(Set.class)))
            .thenAnswer(mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                Set<Index> indices = (Set<Index>) mockInvocation.getArguments()[1];

                final Metadata.Builder b = Metadata.builder(currentState.metadata());
                for (Index index : indices) {
                    b.remove(index.getName());
                }

                return ClusterState.builder(currentState).metadata(b.build()).build();
            });

        return s;
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Tuple<String, Integer> dsTuple : dataStreams) {
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= dsTuple.v2(); backingIndexNumber++) {
                backingIndices.add(createIndexMetadata(DataStream.getBackingIndexName(dsTuple.v1(), backingIndexNumber), true));
            }
            allIndices.addAll(backingIndices);

            DataStream ds = new DataStream(dsTuple.v1(), "@timestamp",
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()), dsTuple.v2());
            builder.put(ds);
        }

        for (String indexName : indexNames) {
            allIndices.add(createIndexMetadata(indexName, false));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden) {
        Settings.Builder b = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.hidden", hidden);

        return IndexMetadata.builder(name)
            .settings(b)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }
}
