/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemDataStreamDescriptor.Type;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataCreateDataStreamServiceTests extends ESTestCase {

    public void testCreateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(
            Collections.singletonList(dataStreamName + "*")
        ).dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate()).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(
            newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    public void testCreateDataStreamWithAliasFromTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        final int aliasCount = randomIntBetween(0, 3);
        Map<String, AliasMetadata> aliases = new HashMap<>(aliasCount);
        for (int k = 0; k < aliasCount; k++) {

            final AliasMetadata am = randomAlias(null);
            aliases.put(am.alias(), am);
        }
        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(
            org.elasticsearch.core.List.of(dataStreamName + "*")
        ).dataStreamTemplate(new DataStreamTemplate()).template(new Template(null, null, aliases)).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(newState.metadata().dataStreamAliases().size(), is(aliasCount));
        for (String aliasName : aliases.keySet()) {
            AliasMetadata expectedAlias = aliases.get(aliasName);
            DataStreamAlias actualAlias = newState.metadata().dataStreamAliases().get(aliasName);
            assertThat(actualAlias, is(notNullValue()));
            assertThat(actualAlias.getName(), equalTo(expectedAlias.alias()));
            assertThat(actualAlias.getFilter(), equalTo(expectedAlias.filter()));
            assertThat(actualAlias.getWriteDataStream(), equalTo(expectedAlias.writeIndex() ? dataStreamName : null));
        }

        assertThat(
            newState.metadata().dataStreamAliases().values().stream().map(DataStreamAlias::getName).toArray(),
            arrayContainingInAnyOrder(new ArrayList<>(aliases.keySet()).toArray())
        );
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getAliases().size(), is(0));
        assertThat(
            newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    public void testCreateDataStreamWithAliasFromComponentTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        final int componentTemplateCount = randomIntBetween(0, 3);
        final int aliasCount = randomIntBetween(0, 3);
        int totalAliasCount = aliasCount;
        Map<String, AliasMetadata> aliases = new HashMap<>();
        for (int k = 0; k < aliasCount; k++) {
            final AliasMetadata am = randomAlias(null);
            aliases.put(am.alias(), am);
        }

        List<String> ctNames = new ArrayList<>();
        List<Map<String, AliasMetadata>> allAliases = new ArrayList<>();
        Metadata.Builder metadataBuilder = Metadata.builder();
        final List<ComponentTemplate> componentTemplates = new ArrayList<>(componentTemplateCount);
        for (int k = 0; k < componentTemplateCount; k++) {
            final String ctName = randomAlphaOfLength(5);
            ctNames.add(ctName);
            final int ctAliasCount = randomIntBetween(0, 3);
            totalAliasCount += ctAliasCount;
            final Map<String, AliasMetadata> ctAliasMap = new HashMap<>(ctAliasCount);
            allAliases.add(ctAliasMap);
            for (int m = 0; m < ctAliasCount; m++) {
                final AliasMetadata am = randomAlias(ctName);
                ctAliasMap.put(am.alias(), am);
            }
            metadataBuilder.put(ctName, new ComponentTemplate(new Template(null, null, ctAliasMap), null, null));
        }
        allAliases.add(aliases);

        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(
            org.elasticsearch.core.List.of(dataStreamName + "*")
        ).dataStreamTemplate(new DataStreamTemplate()).template(new Template(null, null, aliases)).componentTemplates(ctNames).build();

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadataBuilder.put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(newState.metadata().dataStreamAliases().size(), is(totalAliasCount));
        for (Map<String, AliasMetadata> aliasMap : allAliases) {
            for (AliasMetadata alias : aliasMap.values()) {
                DataStreamAlias actualAlias = newState.metadata().dataStreamAliases().get(alias.alias());
                assertThat(actualAlias, is(notNullValue()));
                assertThat(actualAlias.getName(), equalTo(alias.alias()));
                assertThat(actualAlias.getFilter(), equalTo(alias.filter()));
                assertThat(actualAlias.getWriteDataStream(), equalTo(alias.writeIndex() ? dataStreamName : null));
            }
        }

        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getAliases().size(), is(0));
        assertThat(
            newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    private static AliasMetadata randomAlias(String prefix) {
        final String aliasName = (Strings.isNullOrEmpty(prefix) ? "" : prefix + "-") + randomAlphaOfLength(6);
        AliasMetadata.Builder builder = AliasMetadata.newAliasMetadataBuilder(aliasName);
        if (randomBoolean()) {
            builder.filter(
                org.elasticsearch.core.Map.of(
                    "term",
                    org.elasticsearch.core.Map.of("user", org.elasticsearch.core.Map.of("value", randomAlphaOfLength(5)))
                )
            );
        }
        builder.writeIndex(randomBoolean());
        return builder.build();
    }

    public void testCreateSystemDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".system-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().build()).build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(
            dataStreamName,
            systemDataStreamDescriptor(),
            TimeValue.MAX_VALUE,
            TimeValue.ZERO
        );
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isSystem(), is(true));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(
            newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            nullValue()
        );
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(true));
    }

    public void testCreateDuplicateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream = newInstance(
            dataStreamName,
            createTimestampField("@timestamp"),
            Collections.singletonList(idx.getIndex())
        );
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder().dataStreams(Collections.singletonMap(dataStreamName, existingDataStream), Collections.emptyMap()).build()
            )
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);

        ResourceAlreadyExistsException e = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] already exists"));
    }

    public void testCreateDataStreamWithInvalidName() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "_My-da#ta- ,stream-";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(e.getMessage(), containsString("must not contain the following characters"));
    }

    public void testCreateDataStreamWithUppercaseCharacters() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "MAY_NOT_USE_UPPERCASE";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must be lowercase"));
    }

    public void testCreateDataStreamStartingWithPeriod() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".ds-may_not_start_with_ds";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must not start with '.ds-'"));
    }

    public void testCreateDataStreamNoTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(e.getMessage(), equalTo("no matching index template found for data stream [my-data-stream]"));
    }

    public void testCreateDataStreamNoValidTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(
            Collections.singletonList(dataStreamName + "*")
        ).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req)
        );
        assertThat(
            e.getMessage(),
            equalTo("matching index template [template] for data stream [my-data-stream] has no data stream template")
        );
    }

    public static ClusterState createDataStream(final String dataStreamName) throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(
            Collections.singletonList(dataStreamName + "*")
        ).dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate()).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(dataStreamName);
        return MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
    }

    private static MetadataCreateIndexService getMetadataCreateIndexService() throws Exception {
        MetadataCreateIndexService s = mock(MetadataCreateIndexService.class);
        when(s.getSystemIndices()).thenReturn(getSystemIndices());
        when(s.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), anyBoolean())).thenAnswer(
            mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) mockInvocation.getArguments()[1];

                Metadata.Builder b = Metadata.builder(currentState.metadata())
                    .put(
                        IndexMetadata.builder(request.index())
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(request.settings())
                                    .build()
                            )
                            .putMapping("_doc", generateMapping("@timestamp"))
                            .system(getSystemIndices().isSystemName(request.index()))
                            .numberOfShards(1)
                            .numberOfReplicas(1)
                            .build(),
                        false
                    );
                return ClusterState.builder(currentState).metadata(b.build()).build();
            }
        );

        return s;
    }

    private static SystemIndices getSystemIndices() {
        List<Feature> features = org.elasticsearch.core.List.of(
            new Feature(
                "systemFeature",
                "system feature description",
                org.elasticsearch.core.List.of(),
                org.elasticsearch.core.List.of(systemDataStreamDescriptor())
            )
        );

        return new SystemIndices(features);
    }

    private static SystemDataStreamDescriptor systemDataStreamDescriptor() {
        return new SystemDataStreamDescriptor(
            ".system-data-stream",
            "test system datastream",
            Type.EXTERNAL,
            new ComposableIndexTemplate(
                Collections.singletonList(".system-data-stream"),
                null,
                null,
                null,
                null,
                null,
                new DataStreamTemplate()
            ),
            Collections.emptyMap(),
            Collections.singletonList("stack"),
            ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
        );
    }
}
