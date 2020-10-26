/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataCreateDataStreamServiceTests extends ESTestCase {

    public void testCreateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null,
            new ComposableIndexTemplate.DataStreamTemplate(), null);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true"));
    }

    public void testCreateDuplicateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream =
            new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(idx.getIndex()));
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().dataStreams(Map.of(dataStreamName, existingDataStream)).build()).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);

        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] already exists"));
    }

    public void testCreateDataStreamWithInvalidName() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "_My-da#ta- ,stream-";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("must not contain the following characters"));
    }

    public void testCreateDataStreamWithUppercaseCharacters() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "MAY_NOT_USE_UPPERCASE";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must be lowercase"));
    }

    public void testCreateDataStreamStartingWithPeriod() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".may_not_start_with_period";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must not start with '.'"));
    }

    public void testCreateDataStreamNoTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), equalTo("no matching index template found for data stream [my-data-stream]"));
    }

    public void testCreateDataStreamNoValidTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null, null,
            null);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(),
            equalTo("matching index template [template] for data stream [my-data-stream] has no data stream template"));
    }

    public void testCreateDataStreamHidesBackingIndicesAndRemovesAlias() throws Exception {
        String dataStreamName = "foo";
        AliasMetadata alias = AliasMetadata.builder(dataStreamName).build();
        IndexMetadata foo1 = IndexMetadata.builder("foo1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        IndexMetadata foo2 = IndexMetadata.builder("foo2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy")).metadata(
            Metadata.builder()
                .put(foo1, false)
                .put(foo2, false)
                .put("template", new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null,
                    new ComposableIndexTemplate.DataStreamTemplate())))
            .build();

        ClusterState newState = MetadataCreateDataStreamService.createDataStream(getMetadataCreateIndexService(), cs, dataStreamName,
            List.of(foo1, foo2), null);
        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(3));
        List<String> backingIndexNames = ds.getIndices().stream().map(x -> x.getIndex().getName()).collect(Collectors.toList());
        assertThat(backingIndexNames, containsInAnyOrder("foo1", "foo2", DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        for (IndexMetadata im : ds.getIndices()) {
            assertThat(im.getSettings().get("index.hidden"), equalTo("true"));
            assertThat(im.getAliases().size(), equalTo(0));
        }
    }

    public void testCreateDataStreamWithSuppliedWriteIndex() throws Exception {
        String dataStreamName = "foo";
        AliasMetadata alias = AliasMetadata.builder(dataStreamName).build();
        IndexMetadata foo1 = IndexMetadata.builder("foo1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        IndexMetadata foo2 = IndexMetadata.builder("foo2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy")).metadata(
            Metadata.builder()
                .put(foo1, false)
                .put(foo2, false)
                .put("template", new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null,
                    new ComposableIndexTemplate.DataStreamTemplate())))
            .build();

        ClusterState newState = MetadataCreateDataStreamService.createDataStream(getMetadataCreateIndexService(), cs, dataStreamName,
            List.of(foo1), foo2);
        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(2));
        List<String> backingIndexNames = ds.getIndices().stream().map(x -> x.getIndex().getName()).collect(Collectors.toList());
        assertThat(backingIndexNames, containsInAnyOrder("foo1", "foo2"));
        assertThat(ds.getWriteIndex().getIndex().getName(), equalTo("foo2"));
        for (IndexMetadata im : ds.getIndices()) {
            assertThat(im.getSettings().get("index.hidden"), equalTo("true"));
            assertThat(im.getAliases().size(), equalTo(0));
        }
    }

    public void testCreateDataStreamWithoutSuppliedWriteIndex() throws Exception {
        String dataStreamName = "foo";
        AliasMetadata alias = AliasMetadata.builder(dataStreamName).build();
        IndexMetadata foo1 = IndexMetadata.builder("foo1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        IndexMetadata foo2 = IndexMetadata.builder("foo2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(alias)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp", "date"))
            .build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy")).metadata(
            Metadata.builder()
                .put(foo1, false)
                .put(foo2, false)
                .put("template", new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null,
                    new ComposableIndexTemplate.DataStreamTemplate())))
            .build();

        ClusterState newState = MetadataCreateDataStreamService.createDataStream(getMetadataCreateIndexService(), cs, dataStreamName,
            List.of(foo1, foo2), null);
        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(3));
        List<String> backingIndexNames = ds.getIndices().stream().map(x -> x.getIndex().getName()).collect(Collectors.toList());
        assertThat(backingIndexNames, containsInAnyOrder("foo1", "foo2", DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        assertThat(ds.getWriteIndex().getIndex().getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        for (IndexMetadata im : ds.getIndices()) {
            assertThat(im.getSettings().get("index.hidden"), equalTo("true"));
            assertThat(im.getAliases().size(), equalTo(0));
        }
    }

    public static ClusterState createDataStream(final String dataStreamName) throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        ComposableIndexTemplate template = new ComposableIndexTemplate(List.of(dataStreamName + "*"), null, null, null, null, null,
            new ComposableIndexTemplate.DataStreamTemplate(), null);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest req =
            new MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        return MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
    }

    private static MetadataCreateIndexService getMetadataCreateIndexService() throws Exception {
        MetadataCreateIndexService s = mock(MetadataCreateIndexService.class);
        when(s.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), anyBoolean()))
            .thenAnswer(mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) mockInvocation.getArguments()[1];

                Metadata.Builder b = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(request.index())
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(request.settings())
                            .build())
                        .putMapping(generateMapping("@timestamp"))
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build(), false);
                return ClusterState.builder(currentState).metadata(b.build()).build();
            });

        return s;
    }

}
