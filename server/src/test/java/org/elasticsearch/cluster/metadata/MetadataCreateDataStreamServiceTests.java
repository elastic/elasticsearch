/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemDataStreamDescriptor.Type;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.test.ESTestCase;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
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
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            true,
            req,
            ActionListener.noop(),
            false
        );
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).getDataLifecycle(), equalTo(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE));
        assertThat(project.dataStreams().get(dataStreamName).getIndexMode(), nullValue());
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(
            project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    public void testCreateDataStreamLogsdb() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(new Template(Settings.builder().put("index.mode", "logsdb").build(), null, null))
            .dataStreamTemplate(new DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            true,
            req,
            ActionListener.noop(),
            false
        );
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).getIndexMode(), equalTo(IndexMode.LOGSDB));
        assertThat(project.dataStreams().get(dataStreamName).getDataLifecycle(), equalTo(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE));
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(
            project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    public void testCreateDataStreamWithAliasFromTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        final int aliasCount = randomIntBetween(0, 3);
        Map<String, AliasMetadata> aliases = Maps.newMapWithExpectedSize(aliasCount);
        for (int k = 0; k < aliasCount; k++) {
            final AliasMetadata am = randomAlias(null);
            aliases.put(am.alias(), am);
        }
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new DataStreamTemplate())
            .template(new Template(null, null, aliases))
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            false
        );
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.dataStreamAliases().size(), is(aliasCount));
        for (String aliasName : aliases.keySet()) {
            var expectedAlias = aliases.get(aliasName);
            var actualAlias = project.dataStreamAliases().get(aliasName);
            assertThat(actualAlias, is(notNullValue()));
            assertThat(actualAlias.getName(), equalTo(expectedAlias.alias()));
            assertThat(actualAlias.getFilter(dataStreamName), equalTo(expectedAlias.filter()));
            assertThat(actualAlias.getWriteDataStream(), equalTo(expectedAlias.writeIndex() ? dataStreamName : null));
        }

        assertThat(
            project.dataStreamAliases().values().stream().map(DataStreamAlias::getName).toArray(),
            arrayContainingInAnyOrder(new ArrayList<>(aliases.keySet()).toArray())
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getAliases().size(), is(0));
        assertThat(
            project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
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
        var projectId = randomProjectIdOrDefault();
        var metadataBuilder = ProjectMetadata.builder(projectId);
        for (int k = 0; k < componentTemplateCount; k++) {
            final String ctName = randomAlphaOfLength(5);
            ctNames.add(ctName);
            final int ctAliasCount = randomIntBetween(0, 3);
            totalAliasCount += ctAliasCount;
            final var ctAliasMap = new HashMap<String, AliasMetadata>(ctAliasCount);
            allAliases.add(ctAliasMap);
            for (int m = 0; m < ctAliasCount; m++) {
                final AliasMetadata am = randomAlias(ctName);
                ctAliasMap.put(am.alias(), am);
            }
            metadataBuilder.put(ctName, new ComponentTemplate(new Template(null, null, ctAliasMap), null, null));
        }
        allAliases.add(aliases);

        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new DataStreamTemplate())
            .template(new Template(null, null, aliases))
            .componentTemplates(ctNames)
            .build();

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(metadataBuilder.put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            false
        );
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.dataStreamAliases().size(), is(totalAliasCount));
        for (var aliasMap : allAliases) {
            for (var alias : aliasMap.values()) {
                var actualAlias = project.dataStreamAliases().get(alias.alias());
                assertThat(actualAlias, is(notNullValue()));
                assertThat(actualAlias.getName(), equalTo(alias.alias()));
                assertThat(actualAlias.getFilter(dataStreamName), equalTo(alias.filter()));
                assertThat(actualAlias.getWriteDataStream(), equalTo(alias.writeIndex() ? dataStreamName : null));
            }
        }

        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getAliases().size(), is(0));
        assertThat(
            project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(false));
    }

    private static AliasMetadata randomAlias(String prefix) {
        final String aliasName = (Strings.isNullOrEmpty(prefix) ? "" : prefix + "-") + randomAlphaOfLength(6);
        var builder = AliasMetadata.newAliasMetadataBuilder(aliasName);
        if (randomBoolean()) {
            builder.filter(Map.of("term", Map.of("user", Map.of("value", randomAlphaOfLength(5)))));
        }
        builder.writeIndex(randomBoolean());
        return builder.build();
    }

    public void testCreateDataStreamWithFailureStoreInitialized() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            true
        );
        var backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, req.startTime());
        var failureStoreIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, req.startTime());
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.index(backingIndexName), notNullValue());
        assertThat(project.index(backingIndexName).getSettings().get("index.hidden"), equalTo("true"));
        assertThat(project.index(backingIndexName).isSystem(), is(false));
        assertThat(project.index(failureStoreIndexName), notNullValue());
        assertThat(project.index(failureStoreIndexName).getSettings().get("index.hidden"), equalTo("true"));
        assertThat(
            DataStreamFailureStoreDefinition.FAILURE_STORE_DEFINITION_VERSION_SETTING.get(
                project.index(failureStoreIndexName).getSettings()
            ),
            equalTo(DataStreamFailureStoreDefinition.FAILURE_STORE_DEFINITION_VERSION)
        );
        assertThat(project.index(failureStoreIndexName).isSystem(), is(false));
    }

    public void testCreateDataStreamWithFailureStoreUninitialized() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            false
        );
        var backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, req.startTime());
        var failureStoreIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, req.startTime());
        var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.dataStreams().get(dataStreamName).getFailureIndices(), empty());
        assertThat(project.index(backingIndexName), notNullValue());
        assertThat(project.index(backingIndexName).getSettings().get("index.hidden"), equalTo("true"));
        assertThat(project.index(backingIndexName).isSystem(), is(false));
        assertThat(project.index(failureStoreIndexName), nullValue());
    }

    public void testCreateDataStreamWithFailureStoreWithRefreshRate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        var timeValue = randomTimeValue();
        var settings = Settings.builder()
            .put(MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME, timeValue)
            .build();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            settings,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            true
        );
        var backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, req.startTime());
        var failureStoreIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, req.startTime());
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.index(backingIndexName), notNullValue());
        assertThat(project.index(failureStoreIndexName), notNullValue());
        assertThat(
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(project.index(failureStoreIndexName).getSettings()),
            equalTo(timeValue)
        );
    }

    public void testCreateSystemDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".system-data-stream";
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(
            projectId,
            dataStreamName,
            systemDataStreamDescriptor(),
            TimeValue.MAX_VALUE,
            TimeValue.ZERO,
            true
        );
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            false
        );
        final var project = newState.metadata().getProject(projectId);
        assertThat(project.dataStreams().size(), equalTo(1));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).isSystem(), is(true));
        assertThat(project.dataStreams().get(dataStreamName).isHidden(), is(true));
        assertThat(project.dataStreams().get(dataStreamName).isReplicated(), is(false));
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(
            project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true")
        );
        assertThat(project.index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).isSystem(), is(true));
    }

    public void testCreateDuplicateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream = newInstance(dataStreamName, List.of(idx.getIndex()));
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId).dataStreams(Map.of(dataStreamName, existingDataStream), Map.of()).build()
            )
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);

        ResourceAlreadyExistsException e = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] already exists"));
    }

    public void testCreateDataStreamWithInvalidName() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "_My-da#ta- ,stream-";
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(e.getMessage(), containsString("must not contain the following characters"));
    }

    public void testCreateDataStreamWithUppercaseCharacters() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "MAY_NOT_USE_UPPERCASE";
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must be lowercase"));
    }

    public void testCreateDataStreamStartingWithPeriod() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".ds-may_not_start_with_ds";
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must not start with '.ds-'"));
    }

    public void testCreateDataStreamNoTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(e.getMessage(), equalTo("no matching index template found for data stream [my-data-stream]"));
    }

    public void testCreateDataStreamNoValidTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = ComposableIndexTemplate.builder().indexPatterns(List.of(dataStreamName + "*")).build();
        final var projectId = randomProjectIdOrDefault();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(
                metadataCreateIndexService,
                Settings.EMPTY,
                cs,
                randomBoolean(),
                req,
                ActionListener.noop(),
                false
            )
        );
        assertThat(
            e.getMessage(),
            equalTo("matching index template [template] for data stream [my-data-stream] has no data stream template")
        );
    }

    public static ClusterState createDataStream(final String dataStreamName) throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        @FixForMultiProject(description = "This method is exclusively used by TransportBulkActionTests.java")
        final var projectId = Metadata.DEFAULT_PROJECT_ID;
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(projectId, dataStreamName);
        return MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService,
            Settings.EMPTY,
            cs,
            randomBoolean(),
            req,
            ActionListener.noop(),
            false
        );
    }

    private static MetadataCreateIndexService getMetadataCreateIndexService() throws Exception {
        MetadataCreateIndexService s = mock(MetadataCreateIndexService.class);
        when(s.getSystemIndices()).thenReturn(getSystemIndices());
        Answer<Object> objectAnswer = mockInvocation -> {
            ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
            CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) mockInvocation.getArguments()[1];

            ProjectMetadata.Builder b = ProjectMetadata.builder(currentState.metadata().getProject(request.projectId()))
                .put(
                    IndexMetadata.builder(request.index())
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                                .put(request.settings())
                                .build()
                        )
                        .putMapping(generateMapping("@timestamp"))
                        .system(getSystemIndices().isSystemName(request.index()))
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build(),
                    false
                );
            return ClusterState.builder(currentState).putProjectMetadata(b.build()).build();
        };
        when(s.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), anyBoolean(), any()))
            .thenAnswer(objectAnswer);
        when(
            s.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), anyBoolean(), any(), any())
        ).thenAnswer(objectAnswer);

        return s;
    }

    private static SystemIndices getSystemIndices() {
        List<Feature> features = List.of(
            new Feature("systemFeature", "system feature description", List.of(), List.of(systemDataStreamDescriptor()))
        );

        return new SystemIndices(features);
    }

    private static SystemDataStreamDescriptor systemDataStreamDescriptor() {
        return new SystemDataStreamDescriptor(
            ".system-data-stream",
            "test system datastream",
            Type.EXTERNAL,
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(".system-data-stream"))
                .dataStreamTemplate(new DataStreamTemplate())
                .build(),
            Map.of(),
            List.of("stack"),
            "stack",
            ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
        );
    }
}
