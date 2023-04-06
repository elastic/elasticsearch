/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.indices.EmptySystemIndices;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataMigrateToDataStreamServiceTests extends MapperServiceTestCase {

    public void testValidateRequestWithNonexistentAlias() {
        ClusterState cs = ClusterState.EMPTY_STATE;
        String nonExistentAlias = "nonexistent_alias";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.validateRequest(
                cs,
                new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                    nonExistentAlias,
                    TimeValue.ZERO,
                    TimeValue.ZERO
                )
            )
        );
        assertThat(e.getMessage(), containsString("alias [" + nonExistentAlias + "] does not exist"));
    }

    public void testValidateRequestWithFilteredAlias() {
        String filteredAliasName = "filtered_alias";
        AliasMetadata filteredAlias = AliasMetadata.builder(filteredAliasName).filter("""
            {"term":{"user.id":"kimchy"}}
            """).build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("foo")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(filteredAlias)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.validateRequest(
                cs,
                new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                    filteredAliasName,
                    TimeValue.ZERO,
                    TimeValue.ZERO
                )
            )
        );
        assertThat(e.getMessage(), containsString("alias [" + filteredAliasName + "] may not have custom filtering or routing"));
    }

    public void testValidateRequestWithAliasWithRouting() {
        String routedAliasName = "routed_alias";
        AliasMetadata aliasWithRouting = AliasMetadata.builder(routedAliasName).routing("foo").build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("foo")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(aliasWithRouting)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.validateRequest(
                cs,
                new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                    routedAliasName,
                    TimeValue.ZERO,
                    TimeValue.ZERO
                )
            )
        );
        assertThat(e.getMessage(), containsString("alias [" + routedAliasName + "] may not have custom filtering or routing"));
    }

    public void testValidateRequestWithAliasWithoutWriteIndex() {
        String aliasWithoutWriteIndex = "alias";
        AliasMetadata alias1 = AliasMetadata.builder(aliasWithoutWriteIndex).build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("foo1")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo2")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo3")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo4")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.validateRequest(
                cs,
                new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                    aliasWithoutWriteIndex,
                    TimeValue.ZERO,
                    TimeValue.ZERO
                )
            )
        );
        assertThat(e.getMessage(), containsString("alias [" + aliasWithoutWriteIndex + "] must specify a write index"));
    }

    public void testValidateRequest() {
        String aliasName = "alias";
        AliasMetadata alias1 = AliasMetadata.builder(aliasName).build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("foo1")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo2")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo3")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo4")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(AliasMetadata.builder(aliasName).writeIndex(true))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        MetadataMigrateToDataStreamService.validateRequest(
            cs,
            new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(aliasName, TimeValue.ZERO, TimeValue.ZERO)
        );
    }

    public void testValidateRequestWithIndicesWithMultipleAliasReferences() {
        String aliasName = "alias";
        AliasMetadata alias1 = AliasMetadata.builder(aliasName).build();
        AliasMetadata alias2 = AliasMetadata.builder(aliasName + "2").build();
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("foo1")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo2")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .putAlias(alias2)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo3")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .putAlias(alias2)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .put(
                        IndexMetadata.builder("foo4")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .putAlias(alias1)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.validateBackingIndices(cs, aliasName)
        );
        String emsg = e.getMessage();
        assertThat(emsg, containsString("other aliases referencing indices ["));
        assertThat(emsg, containsString("] must be removed before migrating to a data stream"));
        String referencedIndices = emsg.substring(emsg.indexOf('[') + 1, emsg.indexOf(']'));
        Set<String> indices = Strings.commaDelimitedListToSet(referencedIndices);
        assertThat(indices, containsInAnyOrder("foo2", "foo3"));
    }

    public void testCreateDataStreamWithSuppliedWriteIndex() throws Exception {
        String dataStreamName = "foo";
        AliasMetadata alias = AliasMetadata.builder(dataStreamName).build();
        IndexMetadata foo1 = IndexMetadata.builder("foo1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(AliasMetadata.builder(dataStreamName).writeIndex(true).build())
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
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(foo1, false)
                    .put(foo2, false)
                    .put(
                        "template",
                        new ComposableIndexTemplate(
                            List.of(dataStreamName + "*"),
                            null,
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        )
                    )
            )
            .build();

        ClusterState newState = MetadataMigrateToDataStreamService.migrateToDataStream(
            cs,
            this::getMapperService,
            new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                dataStreamName,
                TimeValue.ZERO,
                TimeValue.ZERO
            ),
            getMetadataCreateIndexService(),
            ActionListener.noop()
        );
        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(2));
        List<String> backingIndexNames = ds.getIndices().stream().map(Index::getName).toList();
        assertThat(backingIndexNames, containsInAnyOrder("foo1", "foo2"));
        assertThat(ds.getWriteIndex().getName(), equalTo("foo1"));
        for (Index index : ds.getIndices()) {
            IndexMetadata im = newState.metadata().index(index);
            assertThat(im.getSettings().get("index.hidden"), equalTo("true"));
            assertThat(im.getAliases().size(), equalTo(0));
        }
    }

    public void testCreateDataStreamHidesBackingIndicesAndRemovesAlias() throws Exception {
        String dataStreamName = "foo";
        AliasMetadata alias = AliasMetadata.builder(dataStreamName).build();
        IndexMetadata foo1 = IndexMetadata.builder("foo1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .putAlias(AliasMetadata.builder(dataStreamName).writeIndex(true).build())
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
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(foo1, false)
                    .put(foo2, false)
                    .put(
                        "template",
                        new ComposableIndexTemplate(
                            List.of(dataStreamName + "*"),
                            null,
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        )
                    )
            )
            .build();

        ClusterState newState = MetadataMigrateToDataStreamService.migrateToDataStream(
            cs,
            this::getMapperService,
            new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                dataStreamName,
                TimeValue.ZERO,
                TimeValue.ZERO
            ),
            getMetadataCreateIndexService(),
            ActionListener.noop()
        );
        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(2));
        List<String> backingIndexNames = ds.getIndices().stream().map(Index::getName).toList();
        assertThat(backingIndexNames, containsInAnyOrder("foo1", "foo2"));
        assertThat(ds.getWriteIndex().getName(), equalTo("foo1"));
        for (Index index : ds.getIndices()) {
            IndexMetadata im = newState.metadata().index(index);
            assertThat(im.getSettings().get("index.hidden"), equalTo("true"));
            assertThat(im.getAliases().size(), equalTo(0));
        }
    }

    public void testCreateDataStreamWithoutSuppliedWriteIndex() {
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
        ClusterState cs = ClusterState.builder(new ClusterName("dummy"))
            .metadata(
                Metadata.builder()
                    .put(foo1, false)
                    .put(foo2, false)
                    .put(
                        "template",
                        new ComposableIndexTemplate(
                            List.of(dataStreamName + "*"),
                            null,
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        )
                    )
            )
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataMigrateToDataStreamService.migrateToDataStream(
                cs,
                this::getMapperService,
                new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                    dataStreamName,
                    TimeValue.ZERO,
                    TimeValue.ZERO
                ),
                getMetadataCreateIndexService(),
                ActionListener.noop()
            )
        );
        assertThat(e.getMessage(), containsString("alias [" + dataStreamName + "] must specify a write index"));
    }

    private MapperService getMapperService(IndexMetadata im) {
        try {
            return createMapperService("{\"_doc\": " + im.mapping().source().toString() + "}");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private MetadataCreateIndexService getMetadataCreateIndexService() {
        MetadataCreateIndexService service = mock(MetadataCreateIndexService.class);
        when(service.getSystemIndices()).thenReturn(EmptySystemIndices.INSTANCE);
        return service;
    }
}
