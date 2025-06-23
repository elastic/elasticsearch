/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.health.node.selection.HealthNodeTaskParams;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasks;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SystemIndexMigrationMetadataTests extends ESTestCase {
    public void testParseXContentFormatBeforeMultiProject() throws IOException {
        final String json = org.elasticsearch.core.Strings.format("""
            {
              "meta-data": {
                "version": 54321,
                "cluster_uuid":"aba1aa1ababbbaabaabaab",
                "cluster_uuid_committed":false,
                "persistent_tasks": {
                  "last_allocation_id": 1,
                  "tasks": [
                    {
                      "id": "health-node",
                      "task":{ "health-node": {"params":{}} }
                    },
                    {
                      "id": "upgrade-system-indices",
                      "task":{ "upgrade-system-indices": {"params":{}} }
                    }
                  ]
                },
                "reserved_state":{ }
              }
            }
            """, IndexVersion.current(), IndexVersion.current());

        final var metadata = fromJsonXContentStringWithPersistentTasks(json);

        assertThat(metadata, notNullValue());
        assertThat(metadata.clusterUUID(), is("aba1aa1ababbbaabaabaab"));
        assertThat(metadata.customs().keySet(), contains("cluster_persistent_tasks"));
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(metadata);
        assertThat(clusterTasks.tasks(), hasSize(1));
        assertThat(
            clusterTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getTaskName).toList(),
            containsInAnyOrder("health-node")
        );
        assertThat(metadata.getProject().customs().keySet(), containsInAnyOrder("persistent_tasks", "index-graveyard"));
        final var projectTasks = PersistentTasksCustomMetadata.get(metadata.getProject());
        assertThat(
            projectTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getTaskName).toList(),
            containsInAnyOrder("upgrade-system-indices")
        );
        assertThat(clusterTasks.getLastAllocationId(), equalTo(projectTasks.getLastAllocationId()));
    }

    private Metadata fromJsonXContentStringWithPersistentTasks(String json) throws IOException {
        List<NamedXContentRegistry.Entry> registry = new ArrayList<>();
        registry.addAll(ClusterModule.getNamedXWriteables());
        registry.addAll(IndicesModule.getNamedXContents());
        registry.addAll(HealthNodeTaskExecutor.getNamedXContentParsers());
        registry.addAll(SystemIndexMigrationExecutor.getNamedXContentParsers());

        final var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
        final var healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
            clusterService,
            mock(PersistentTasksService.class),
            Settings.EMPTY,
            ClusterSettings.createBuiltInClusterSettings()
        );
        final var systemIndexMigrationExecutor = new SystemIndexMigrationExecutor(
            mock(Client.class),
            clusterService,
            mock(SystemIndices.class),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            mock(ThreadPool.class)
        );
        new PersistentTasksExecutorRegistry(List.of(healthNodeTaskExecutor, systemIndexMigrationExecutor));

        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(registry));
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(config, json)) {
            return Metadata.fromXContent(parser);
        }
    }

    public void testMultiProjectXContent() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final List<ProjectMetadata> projects = randomList(
            1,
            5,
            () -> ProjectMetadata.builder(randomUniqueProjectId())
                .putCustom(
                    PersistentTasksCustomMetadata.TYPE,
                    new PersistentTasksCustomMetadata(
                        lastAllocationId,
                        Map.of(
                            SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                            new PersistentTasksCustomMetadata.PersistentTask<>(
                                SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                                SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                                new SystemIndexMigrationTaskParams(),
                                lastAllocationId,
                                PersistentTasks.INITIAL_ASSIGNMENT
                            )
                        )
                    )
                )
                .build()
        );

        Metadata.Builder metadataBuilder = Metadata.builder()
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId + 1,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId + 1,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            );
        for (ProjectMetadata project : projects) {
            metadataBuilder.put(project);
        }
        final Metadata originalMeta = metadataBuilder.build();

        ToXContent.Params p = new ToXContent.MapParams(
            Map.of("multi-project", "true", Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)
        );

        final BytesReference bytes = toXContentBytes(originalMeta, p);

        // XContent with multi-project=true has separate cluster and project tasks
        final var objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, bytes);
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks"), notNullValue());
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.last_allocation_id"), equalTo(lastAllocationId + 1));
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.tasks"), hasSize(1));
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.tasks.0.id"), equalTo(HealthNode.TASK_NAME));
        assertThat(objectPath.evaluate("meta-data.projects"), hasSize(projects.size()));
        assertThat(IntStream.range(0, projects.size()).mapToObj(i -> {
            try {
                return (String) objectPath.evaluate("meta-data.projects." + i + ".id");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toUnmodifiableSet()),
            equalTo(projects.stream().map(pp -> pp.id().id()).collect(Collectors.toUnmodifiableSet()))
        );

        for (int i = 0; i < projects.size(); i++) {
            assertThat(objectPath.evaluate("meta-data.projects." + i + ".persistent_tasks"), notNullValue());
            assertThat(objectPath.evaluate("meta-data.projects." + i + ".persistent_tasks.last_allocation_id"), equalTo(lastAllocationId));
            assertThat(objectPath.evaluate("meta-data.projects." + i + ".persistent_tasks.tasks"), hasSize(1));
            assertThat(
                objectPath.evaluate("meta-data.projects." + i + ".persistent_tasks.tasks.0.id"),
                equalTo(SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME)
            );
        }

        Metadata fromXContentMeta = fromJsonXContentStringWithPersistentTasks(bytes.utf8ToString());
        assertThat(fromXContentMeta.projects().keySet(), equalTo(originalMeta.projects().keySet()));
        for (var project : fromXContentMeta.projects().values()) {
            final var projectTasks = PersistentTasksCustomMetadata.get(project);
            assertThat(projectTasks.getLastAllocationId(), equalTo(lastAllocationId));
            assertThat(projectTasks.taskMap().keySet(), equalTo(Set.of(SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME)));
        }
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(fromXContentMeta);
        assertThat(clusterTasks.getLastAllocationId(), equalTo(lastAllocationId + 1));
        assertThat(clusterTasks.taskMap().keySet(), equalTo(Set.of(HealthNode.TASK_NAME)));
    }

    public void testDefaultProjectXContentWithPersistentTasks() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final var originalMeta = Metadata.builder()
            .clusterUUID(randomUUID())
            .clusterUUIDCommitted(true)
            .put(
                ProjectMetadata.builder(ProjectId.DEFAULT)
                    .putCustom(
                        PersistentTasksCustomMetadata.TYPE,
                        new PersistentTasksCustomMetadata(
                            lastAllocationId,
                            Map.of(
                                SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                                new PersistentTasksCustomMetadata.PersistentTask<>(
                                    SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                                    SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME,
                                    new SystemIndexMigrationTaskParams(),
                                    lastAllocationId,
                                    PersistentTasks.INITIAL_ASSIGNMENT
                                )
                            )
                        )
                    )
            )
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId + 1,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId + 1,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            )
            .build();

        // For single project metadata, XContent output should combine the cluster and project tasks
        final ToXContent.Params p = new ToXContent.MapParams(
            Map.ofEntries(Map.entry("multi-project", "false"), Map.entry(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_SNAPSHOT))
        );
        final BytesReference bytes = toXContentBytes(originalMeta, p);

        final var objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, bytes);
        // No cluster_persistent_tasks for single project output, it is combined with persistent_tasks
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks"), nullValue());
        // The combined lastAllocationId is the max between cluster and project tasks
        assertThat(objectPath.evaluate("meta-data.persistent_tasks.last_allocation_id"), equalTo(lastAllocationId + 1));
        assertThat(objectPath.evaluate("meta-data.persistent_tasks.tasks"), hasSize(2));
        assertThat(
            Set.of(
                objectPath.evaluate("meta-data.persistent_tasks.tasks.0.id"),
                objectPath.evaluate("meta-data.persistent_tasks.tasks.1.id")
            ),
            equalTo(Set.of(HealthNode.TASK_NAME, SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME))
        );

        // Deserialize from the XContent should separate cluster and project tasks
        final Metadata fromXContentMeta = fromJsonXContentStringWithPersistentTasks(bytes.utf8ToString());
        assertThat(fromXContentMeta.projects().keySet(), equalTo(Set.of(ProjectId.DEFAULT)));
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(fromXContentMeta);
        assertThat(clusterTasks, notNullValue());
        assertThat(clusterTasks.getLastAllocationId(), equalTo(lastAllocationId + 1));
        assertThat(
            clusterTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getId).toList(),
            contains(HealthNode.TASK_NAME)
        );
        final var projectTasks = PersistentTasksCustomMetadata.get(fromXContentMeta.getProject(ProjectId.DEFAULT));
        assertThat(projectTasks, notNullValue());
        assertThat(projectTasks.getLastAllocationId(), equalTo(lastAllocationId + 1));
        assertThat(
            projectTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getId).toList(),
            contains(SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME)
        );
    }

    private static BytesReference toXContentBytes(Metadata metadata, ToXContent.Params params) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, params);
        builder.endObject();
        return BytesReference.bytes(builder);
    }
}
