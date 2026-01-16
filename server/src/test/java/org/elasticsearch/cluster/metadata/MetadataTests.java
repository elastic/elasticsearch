/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.health.node.selection.HealthNodeTaskParams;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasks;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_API;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataTests extends ESTestCase {

    private static final TransportVersion MULTI_PROJECT = TransportVersion.fromName("multi_project");

    public void testUnknownFieldClusterMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("meta-data").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> Metadata.Builder.fromXContent(parser));
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testUnknownFieldIndexMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("index_name").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> IndexMetadata.Builder.fromXContent(parser));
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testMetadataGlobalStateChangesOnIndexDeletions() {
        final var projectId = randomProjectIdOrDefault();
        IndexGraveyard.Builder builder = IndexGraveyard.builder();
        builder.addTombstone(new Index("idx1", UUIDs.randomBase64UUID()));
        final Metadata metadata1 = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(builder.build())).build();
        builder = IndexGraveyard.builder(metadata1.getProject(projectId).indexGraveyard());
        builder.addTombstone(new Index("idx2", UUIDs.randomBase64UUID()));
        final Metadata metadata2 = Metadata.builder(metadata1)
            .put(ProjectMetadata.builder(metadata1.getProject(projectId)).indexGraveyard(builder.build()))
            .build();
        assertFalse("metadata not equal after adding index deletions", Metadata.isGlobalStateEquals(metadata1, metadata2));
        final Metadata metadata3 = Metadata.builder(metadata2).build();
        assertTrue("metadata equal when not adding index deletions", Metadata.isGlobalStateEquals(metadata2, metadata3));
    }

    public void testXContentWithIndexGraveyard() throws IOException {
        @FixForMultiProject // XContent serialization and parsing with a random project ID currently only works when serializing in MP mode
        final var projectId = ProjectId.DEFAULT;
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(graveyard)).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(originalMeta).toXContent(builder, formatParams());
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(
                fromXContentMeta.getProject(projectId).indexGraveyard(),
                equalTo(originalMeta.getProject(projectId).indexGraveyard())
            );
        }
    }

    public void testXContentClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(originalMeta).toXContent(builder, formatParams());
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(fromXContentMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
            assertThat(fromXContentMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
        }
    }

    public void testSerializationClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
        assertThat(fromStreamMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
    }

    public void testMetadataGlobalStateChangesOnClusterUUIDChanges() {
        final Metadata metadata1 = Metadata.builder().clusterUUID(UUIDs.randomBase64UUID()).clusterUUIDCommitted(randomBoolean()).build();
        final Metadata metadata2 = Metadata.builder(metadata1).clusterUUID(UUIDs.randomBase64UUID()).build();
        final Metadata metadata3 = Metadata.builder(metadata1).clusterUUIDCommitted(metadata1.clusterUUIDCommitted() == false).build();
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata3));
        final Metadata metadata4 = Metadata.builder(metadata2).clusterUUID(metadata1.clusterUUID()).build();
        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata4));
    }

    public void testMetadataGlobalStateChangesOnProjectChanges() {
        final Metadata metadata1 = Metadata.builder().build();
        final Metadata metadata2 = Metadata.builder(metadata1).put(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
        final Metadata metadata3 = Metadata.builder(metadata1)
            .put(
                ProjectMetadata.builder(randomUniqueProjectId())
                    .put(IndexMetadata.builder("some-index").settings(indexSettings(IndexVersion.current(), 1, 1)))
                    .build()
            )
            .build();
        // A project with a ProjectCustom.
        final Metadata metadata4 = Metadata.builder(metadata1)
            .put(
                ProjectMetadata.builder(randomUniqueProjectId())
                    .put("template", new ComponentTemplate(new Template(null, null, null), null, null))
                    .build()
            )
            .build();
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertTrue(Metadata.isGlobalStateEquals(metadata2, Metadata.builder(metadata1).projectMetadata(metadata2.projects()).build()));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata3));
        assertTrue(Metadata.isGlobalStateEquals(metadata3, Metadata.builder(metadata1).projectMetadata(metadata3.projects()).build()));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata4));
        assertTrue(Metadata.isGlobalStateEquals(metadata4, Metadata.builder(metadata1).projectMetadata(metadata4.projects()).build()));
    }

    private static CoordinationMetadata.VotingConfiguration randomVotingConfig() {
        return new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    private Set<VotingConfigExclusion> randomVotingConfigExclusions() {
        final int size = randomIntBetween(0, 10);
        final Set<VotingConfigExclusion> nodes = Sets.newHashSetWithExpectedSize(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
    }

    public void testXContentWithCoordinationMetadata() throws IOException {
        CoordinationMetadata originalMeta = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );

        Metadata metadata = Metadata.builder().coordinationMetadata(originalMeta).build();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, formatParams());
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetadata fromXContentMeta = Metadata.fromXContent(parser).coordinationMetadata();
            assertThat(fromXContentMeta, equalTo(originalMeta));
        }
    }

    public void testParseXContentFormatBeforeMultiProject() throws IOException {
        final String json = org.elasticsearch.core.Strings.format("""
            {
              "meta-data": {
                "version": 54321,
                "cluster_uuid":"aba1aa1ababbbaabaabaab",
                "cluster_uuid_committed":false,
                "cluster_coordination":{
                  "term":1,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions":[]
                },
                "templates":{
                  "template":{
                    "order":0,
                    "index_patterns":["index-*"],
                    "settings":{
                      "something":true
                    },
                    "mappings":{ },
                    "aliases":{ }
                  }
                },
                "persistent_tasks": {
                  "last_allocation_id": 1,
                  "tasks": [
                    {
                      "id": "health-node",
                      "task":{ "health-node": {"params":{}} }
                    }
                  ]
                },
                "index-graveyard":{
                  "tombstones":[{
                    "index":{
                      "index_name":"old-index",
                      "index_uuid":"index_index_index_1234"
                    },
                    "delete_date_in_millis":1717170000000
                  }]
                },
                "desired_nodes":{
                  "latest": {
                    "history_id": "test",
                    "version": 1,
                    "nodes": [{
                      "settings":{ "node":{"name":"node-dn1"} },
                      "processors": 3.5,
                      "memory": "32gb",
                      "storage": "256gb",
                      "status": 0
                    }]
                  }
                },
                "component_template":{
                  "component_template":{
                    "sample-template":{
                      "template":{
                        "mappings":"REZMAKtWKijKL0gtKslMLVayqlaKCndxiwAxSioLUpWslLJTK8vzi1KUamtrAQ=="
                      },
                      "_meta":{
                        "awesome":true
                      },
                      "deprecated":false
                    }
                  }
                },
                "repositories": {
                  "my-repo": {
                    "type": "fs",
                    "uuid": "_my-repo-uuid_",
                    "settings": {
                      "location": "backup"
                    },
                    "generation": 42,
                    "pending_generation": 42
                  }
                },
                "reserved_state":{ }
              }
            }
            """, IndexVersion.current(), IndexVersion.current());

        final var metadata = fromJsonXContentStringWithPersistentTasks(json);

        assertThat(metadata, notNullValue());
        assertThat(metadata.clusterUUID(), is("aba1aa1ababbbaabaabaab"));
        assertThat(metadata.customs().keySet(), containsInAnyOrder("desired_nodes", "cluster_persistent_tasks"));
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(metadata);
        assertThat(clusterTasks.tasks(), hasSize(1));
        assertThat(
            clusterTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getTaskName).toList(),
            containsInAnyOrder("health-node")
        );
        assertThat(
            metadata.getProject(ProjectId.DEFAULT).customs().keySet(),
            containsInAnyOrder("persistent_tasks", "index-graveyard", "component_template", "repositories")
        );
        assertThat(metadata.customs(), not(hasKey("repositories")));
        final var repositoriesMetadata = RepositoriesMetadata.get(metadata.getProject(ProjectId.DEFAULT));
        assertThat(
            repositoriesMetadata.repositories(),
            equalTo(
                List.of(
                    new RepositoryMetadata("my-repo", "_my-repo-uuid_", "fs", Settings.builder().put("location", "backup").build(), 42, 42)
                )
            )
        );
    }

    public void testParseXContentFormatBeforeRepositoriesMetadataMigration() throws IOException {
        final String json = org.elasticsearch.core.Strings.format("""
            {
              "meta-data": {
                "version": 54321,
                "cluster_uuid":"aba1aa1ababbbaabaabaab",
                "cluster_uuid_committed":false,
                "cluster_coordination":{
                  "term":1,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions":[]
                },
                "projects" : [
                  {
                    "id" : "default",
                    "templates" : {
                      "template" : {
                        "order" : 0,
                        "index_patterns" : [
                          "pattern1",
                          "pattern2"
                        ],
                        "mappings" : {
                          "key1" : { }
                        },
                        "aliases" : { }
                      }
                    },
                    "index-graveyard" : {
                      "tombstones" : [ ]
                    },
                    "reserved_state" : { }
                  },
                  {
                    "id" : "another_project",
                    "templates" : {
                      "template" : {
                        "order" : 0,
                        "index_patterns" : [
                          "pattern1",
                          "pattern2"
                        ],
                        "mappings" : {
                          "key1" : { }
                        },
                        "aliases" : { }
                      }
                    },
                    "index-graveyard" : {
                      "tombstones" : [ ]
                    },
                    "reserved_state" : { }
                  }
                ],
                "repositories": {
                  "my-repo": {
                    "type": "fs",
                    "uuid": "_my-repo-uuid_",
                    "settings": {
                      "location": "backup"
                    },
                    "generation": 42,
                    "pending_generation": 42
                  }
                },
                "reserved_state":{ }
              }
            }
            """, IndexVersion.current(), IndexVersion.current());

        final Metadata metadata = fromJsonXContentStringWithPersistentTasks(json);
        assertThat(metadata, notNullValue());
        assertThat(metadata.clusterUUID(), is("aba1aa1ababbbaabaabaab"));

        assertThat(metadata.projects().keySet(), containsInAnyOrder(ProjectId.fromId("default"), ProjectId.fromId("another_project")));
        assertThat(metadata.customs(), not(hasKey("repositories")));
        final var repositoriesMetadata = RepositoriesMetadata.get(metadata.getProject(ProjectId.DEFAULT));
        assertThat(
            repositoriesMetadata.repositories(),
            equalTo(
                List.of(
                    new RepositoryMetadata("my-repo", "_my-repo-uuid_", "fs", Settings.builder().put("location", "backup").build(), 42, 42)
                )
            )
        );
        assertThat(metadata.getProject(ProjectId.fromId("another_project")).customs(), not(hasKey("repositories")));
    }

    private Metadata fromJsonXContentStringWithPersistentTasks(String json) throws IOException {
        List<NamedXContentRegistry.Entry> registry = new ArrayList<>();
        registry.addAll(ClusterModule.getNamedXWriteables());
        registry.addAll(IndicesModule.getNamedXContents());
        registry.addAll(HealthNodeTaskExecutor.getNamedXContentParsers());

        final var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
        final var healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
            clusterService,
            mock(PersistentTasksService.class),
            Settings.EMPTY,
            ClusterSettings.createBuiltInClusterSettings()
        );
        new PersistentTasksExecutorRegistry(List.of(healthNodeTaskExecutor));

        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(registry));
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(config, json)) {
            return Metadata.fromXContent(parser);
        }
    }

    public void testGlobalStateEqualsCoordinationMetadata() {
        CoordinationMetadata coordinationMetadata1 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata1 = Metadata.builder().coordinationMetadata(coordinationMetadata1).build();
        CoordinationMetadata coordinationMetadata2 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata2 = Metadata.builder().coordinationMetadata(coordinationMetadata2).build();

        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata1));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
    }

    public void testSerializationWithIndexGraveyard() throws IOException {
        final var projectId = randomProjectIdOrDefault();
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(graveyard)).build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.getProject(projectId).indexGraveyard(), equalTo(originalMeta.getProject(projectId).indexGraveyard()));
    }

    private static IndexMetadata.Builder buildIndexMetadata(String name, String alias, Boolean writeIndex) {
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()))
            .creationDate(randomNonNegativeLong())
            .putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex))
            .numberOfShards(1)
            .numberOfReplicas(0);
    }

    public void testTransientSettingsOverridePersistentSettings() {
        final Setting<String> setting = Setting.simpleString("key");
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(setting.getKey(), "persistent-value").build())
            .transientSettings(Settings.builder().put(setting.getKey(), "transient-value").build())
            .build();
        assertThat(setting.get(metadata.settings()), equalTo("transient-value"));
    }

    public void testBuilderRejectsNullCustom() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        assertThat(
            expectThrows(NullPointerException.class, () -> builder.putCustom(key, (Metadata.ClusterCustom) null)).getMessage(),
            containsString(key)
        );
    }

    public void testBuilderRejectsNullInCustoms() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        {
            final Map<String, Metadata.ClusterCustom> map = new HashMap<>();
            map.put(key, null);
            assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
        }
    }

    public void testCopyAndUpdate() throws IOException {
        var metadata = Metadata.builder().clusterUUID(UUIDs.base64UUID()).build();
        var newClusterUuid = UUIDs.base64UUID();

        var copy = metadata.copyAndUpdate(builder -> builder.clusterUUID(newClusterUuid));

        assertThat(copy, not(sameInstance(metadata)));
        assertThat(copy.clusterUUID(), equalTo(newClusterUuid));
    }

    public void testBuilderRemoveClusterCustomIf() {
        var custom1 = new TestClusterCustomMetadata();
        var custom2 = new TestClusterCustomMetadata();
        var builder = Metadata.builder();
        builder.putCustom("custom1", custom1);
        builder.putCustom("custom2", custom2);

        builder.removeCustomIf((key, value) -> Objects.equals(key, "custom1"));

        var metadata = builder.build();
        assertThat(metadata.custom("custom1"), nullValue());
        assertThat(metadata.custom("custom2"), sameInstance(custom2));
    }

    public void testSerialization() throws IOException {
        final Metadata orig = randomMetadata();
        final BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertTrue(Metadata.isGlobalStateEquals(orig, fromStreamMeta));
    }

    public void testMultiProjectSerialization() throws IOException {
        ProjectMetadata project1 = randomProject(randomProjectIdOrDefault(), 1);
        ProjectMetadata project2 = randomProject(randomUniqueProjectId(), randomIntBetween(2, 10));
        Metadata metadata = randomMetadata(List.of(project1, project2));
        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // check it deserializes ok
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        Metadata fromStreamMeta = Metadata.readFrom(new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry));

        // check it matches the original object
        assertThat(fromStreamMeta.projects(), aMapWithSize(2));
        for (var original : List.of(project1, project2)) {
            assertThat(fromStreamMeta.projects(), hasKey(original.id()));
            final ProjectMetadata fromStreamProject = fromStreamMeta.getProject(original.id());
            assertThat("For project " + original.id(), fromStreamProject.indices().size(), equalTo(original.indices().size()));
            assertThat("For project " + original.id(), fromStreamProject.dataStreams().size(), equalTo(original.dataStreams().size()));
            assertThat("For project " + original.id(), fromStreamProject.templates().size(), equalTo(original.templates().size()));
            assertThat("For project " + original.id(), fromStreamProject.templatesV2().size(), equalTo(original.templatesV2().size()));
            original.indices().forEach((name, value) -> {
                assertThat(fromStreamProject.indices(), hasKey(name));
                assertThat(fromStreamProject.index(name), equalTo(value));
            });
            original.dataStreams().forEach((name, value) -> {
                assertThat(fromStreamProject.dataStreams(), hasKey(name));
                assertThat(fromStreamProject.dataStreams().get(name), equalTo(value));
            });
        }
    }

    public void testUnableToSerializeNonDefaultProjectBeforeMultiProject() {
        final var projectId = randomUniqueProjectId();
        Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(projectId)).build();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersionUtils.getPreviousVersion(MULTI_PROJECT));
            var e = assertThrows(UnsupportedOperationException.class, () -> metadata.writeTo(output));
            assertEquals("There is 1 project, but it has id [" + projectId + "] rather than default", e.getMessage());
        }
    }

    public void testGetNonExistingProjectThrows() {
        final List<ProjectMetadata> projects = IntStream.range(0, between(1, 3))
            .mapToObj(i -> randomProject(ProjectId.fromId("p_" + i), between(0, 5)))
            .toList();
        final Metadata metadata = randomMetadata(projects);
        expectThrows(IllegalArgumentException.class, () -> metadata.getProject(randomProjectIdOrDefault()));
    }

    public void testEmptyDiffReturnsSameInstance() throws IOException {
        final Metadata instance = randomMetadata();
        Diff<Metadata> diff = instance.diff(instance);
        assertSame(instance, diff.apply(instance));
        final BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);
        final Diff<Metadata> deserializedDiff = Metadata.readDiffFrom(out.bytes().streamInput());
        assertSame(instance, deserializedDiff.apply(instance));
    }

    public void testMultiProjectXContent() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final List<ProjectMetadata> projects = randomList(1, 5, () -> randomProject(randomUniqueProjectId(), randomIntBetween(1, 3)))
            .stream()
            .map(
                project -> ProjectMetadata.builder(project)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            List.of(
                                new RepositoryMetadata(
                                    "backup",
                                    "uuid-" + project.id().id(),
                                    "fs",
                                    Settings.builder().put("location", project.id().id()).build()
                                )
                            )
                        )
                    )
                    .build()
            )
            .toList();

        final Metadata originalMeta = Metadata.builder(randomMetadata(projects))
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

        Metadata fromXContentMeta = fromJsonXContentStringWithPersistentTasks(bytes.utf8ToString());
        assertThat(fromXContentMeta.projects().keySet(), equalTo(originalMeta.projects().keySet()));
        for (var project : fromXContentMeta.projects().values()) {
            assertThat(
                RepositoriesMetadata.get(project).repositories(),
                equalTo(
                    List.of(
                        new RepositoryMetadata(
                            "backup",
                            "uuid-" + project.id().id(),
                            "fs",
                            Settings.builder().put("location", project.id().id()).build()
                        )
                    )
                )
            );
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
        assertThat(objectPath.evaluate("meta-data.persistent_tasks.tasks"), hasSize(1));

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
    }

    public void testSingleNonDefaultProjectXContent() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final var indexVersion = IndexVersion.current();
        // When ClusterStateAction acts in a project scope, it returns cluster state metadata that has a single project that may
        // not have the default project-id. We need to be able to convert this to XContent in the Rest response
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("c8af967d644b3219"))
            .put(IndexMetadata.builder("index-1").settings(indexSettings(indexVersion, 1, 1)).build(), false)
            .put(IndexMetadata.builder("index-2").settings(indexSettings(indexVersion, 2, 2)).build(), false)
            .build();
        final Metadata metadata = Metadata.builder()
            .clusterUUID("afSSOgAAQAC8BuQTAAAAAA")
            .clusterUUIDCommitted(true)
            .put(project)
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            )
            .build();
        final ToXContent.Params p = new ToXContent.MapParams(
            Map.ofEntries(Map.entry("multi-project", "false"), Map.entry(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API))
        );
        var expected = Strings.format("""
            {
              "metadata": {
                "cluster_uuid": "afSSOgAAQAC8BuQTAAAAAA",
                "cluster_uuid_committed":true,
                "cluster_coordination": {
                  "term":0,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions": []
                },
                "templates": {},
                "indices": {
                  "index-1": {
                    "version": 1,
                    "transport_version": "0",
                    "mapping_version": 1,
                    "settings_version": 1,
                    "aliases_version": 1,
                    "routing_num_shards": 1,
                    "state": "open",
                    "settings": {
                      "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "1",
                        "version": {
                          "created": "%s"
                        }
                      }
                    },
                    "mappings": {},
                    "aliases": [],
                    "primary_terms": {
                      "0": 0
                    },
                    "in_sync_allocations": {
                      "0": []
                    },
                    "rollover_info": {},
                    "mappings_updated_version": %s,
                    "system": false,
                    "timestamp_range": {
                      "shards": []
                    },
                    "event_ingested_range": {
                      "shards": []
                    }
                  },
                  "index-2": {
                    "version": 1,
                    "transport_version": "0",
                    "mapping_version": 1,
                    "settings_version": 1,
                    "aliases_version": 1,
                    "routing_num_shards": 2,
                    "state": "open",
                    "settings": {
                      "index": {
                        "number_of_shards": "2",
                        "number_of_replicas": "2",
                        "version": {
                          "created": "%s"
                        }
                      }
                    },
                    "mappings": {},
                    "aliases": [],
                    "primary_terms": {
                      "0": 0,
                      "1": 0
                    },
                    "in_sync_allocations": {
                      "1": [],
                      "0": []
                    },
                    "rollover_info": {},
                    "mappings_updated_version": %s,
                    "system": false,
                    "timestamp_range": {
                      "shards": []
                    },
                    "event_ingested_range": {
                      "shards": []
                    }
                  }
                },
                "index-graveyard": {
                  "tombstones": []
                },
                "reserved_state": {},
                "persistent_tasks": {
                  "last_allocation_id": %s,
                  "tasks": [
                    {
                      "id": "health-node",
                      "task": { "health-node": {"params":{}} },
                      "assignment": {
                        "explanation": "waiting for initial assignment",
                        "executor_node": null
                      },
                      "allocation_id": %s
                    }
                  ]
                }
              }
            }
            """, indexVersion, indexVersion, indexVersion, indexVersion, lastAllocationId, lastAllocationId);
        assertToXContentEquivalent(new BytesArray(expected), toXContentBytes(metadata, p), XContentType.JSON);
    }

    private static BytesReference toXContentBytes(Metadata metadata, ToXContent.Params params) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, params);
        builder.endObject();
        return BytesReference.bytes(builder);
    }

    public void testChunkedToXContent() {
        AbstractChunkedSerializingTestCase.assertChunkCount(randomMetadata(randomInt(10)), MetadataTests::expectedChunkCount);
    }

    private static int expectedChunkCount(Metadata metadata) {
        return expectedChunkCount(ToXContent.EMPTY_PARAMS, metadata);
    }

    public static int expectedChunkCount(ToXContent.Params params, Metadata metadata) {
        final var context = Metadata.XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));

        // 2 chunks at the beginning
        long chunkCount = 2;
        // 1 optional chunk for persistent settings
        if (context != Metadata.XContentContext.API && metadata.persistentSettings().isEmpty() == false) {
            chunkCount += 1;
        }

        for (Metadata.ClusterCustom custom : metadata.customs().values()) {
            chunkCount += 2;
            if (custom instanceof DesiredNodesMetadata) {
                chunkCount += checkChunkSize(custom, params, 1);
            } else if (custom instanceof NodesShutdownMetadata nodesShutdownMetadata) {
                chunkCount += checkChunkSize(custom, params, 2 + nodesShutdownMetadata.getAll().size());
            } else {
                // could be anything, we have to just try it
                chunkCount += count(custom.toXContentChunked(params));
            }
        }
        if (context != Metadata.XContentContext.API || params.paramAsBoolean("multi-project", false)) {
            chunkCount += 2; // start/end "projects":[]
            chunkCount += 3L * metadata.projects().size(); // open/close + id-field
        }

        chunkCount += metadata.projects()
            .values()
            .stream()
            .mapToInt(project -> ProjectMetadataTests.expectedChunkCount(params, project))
            .sum();

        int reservedStateSize = metadata.reservedStateMetadata().size();

        // 2 chunks for wrapping reserved state + 1 chunk for each item
        chunkCount += 2 + reservedStateSize;
        // 1 chunk to close metadata
        chunkCount += 1;

        return Math.toIntExact(chunkCount);
    }

    protected static long count(Iterator<?> it) {
        long count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        return count;
    }

    protected static long checkChunkSize(ChunkedToXContent custom, ToXContent.Params params, long expectedSize) {
        long actualSize = count(custom.toXContentChunked(params));
        assertThat(actualSize, equalTo(expectedSize));
        return actualSize;
    }

    /**
     * With this test we ensure that we consider whether a new field added to Metadata should be checked
     * in Metadata.isGlobalStateEquals. We force the instance fields to be either in the checked list
     * or in the excluded list.
     * <p>
     * This prevents from accidentally forgetting that a new field should be checked in isGlobalStateEquals.
     */
    @SuppressForbidden(reason = "need access to all fields, they are mostly private")
    public void testEnsureMetadataFieldCheckedForGlobalStateChanges() {
        Set<String> checkedForGlobalStateChanges = Set.of(
            "coordinationMetadata",
            "persistentSettings",
            "hashesOfConsistentSettings",
            "templates",
            "clusterUUID",
            "clusterUUIDCommitted",
            "customs",
            "reservedStateMetadata"
        );
        Set<String> excludedFromGlobalStateCheck = Set.of(
            "projectMetadata",
            "version",
            "transientSettings",
            "settings",
            "indices",
            "aliasedIndices",
            "totalNumberOfShards",
            "totalOpenIndexShards",
            "allIndices",
            "visibleIndices",
            "allOpenIndices",
            "visibleOpenIndices",
            "allClosedIndices",
            "visibleClosedIndices",
            "indicesLookup",
            "mappingsByHash",
            "oldestIndexVersion",
            "projectLookup"
        );

        var diff = new HashSet<>(checkedForGlobalStateChanges);
        diff.removeAll(excludedFromGlobalStateCheck);

        // sanity check that the two field sets are mutually exclusive
        assertEquals(checkedForGlobalStateChanges, diff);

        // any declared non-static field in metadata must be either in the list of fields
        // we check for global state changes, or in the fields excluded from the global state check.
        var unclassifiedFields = Arrays.stream(Metadata.class.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) == false)
            .map(Field::getName)
            .filter(n -> (checkedForGlobalStateChanges.contains(n) || excludedFromGlobalStateCheck.contains(n)) == false)
            .collect(Collectors.toSet());
        assertThat(unclassifiedFields, empty());
    }

    public void testGetSingleProjectWithCustom() {
        var type = IngestMetadata.TYPE;
        {
            Metadata metadata = Metadata.builder().build();
            assertNull(metadata.getSingleProjectCustom(type));
            assertNull(metadata.getSingleProjectWithCustom(type));
        }
        {
            Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
            assertNull(metadata.getSingleProjectCustom(type));
            assertNull(metadata.getSingleProjectWithCustom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertEquals(ingestMetadata, metadata.getSingleProjectCustom(type));
            assertEquals(ingestMetadata, metadata.getSingleProjectWithCustom(type).custom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()))
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertEquals(ingestMetadata, metadata.getSingleProjectCustom(type));
            assertEquals(ingestMetadata, metadata.getSingleProjectWithCustom(type).custom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, new IngestMetadata(Map.of())))
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertThrows(UnsupportedOperationException.class, () -> metadata.getSingleProjectCustom(type));
            assertThrows(UnsupportedOperationException.class, () -> metadata.getSingleProjectWithCustom(type));
        }
    }

    public void testProjectLookupWithSingleProject() {
        final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

        final int numberOfIndices = randomIntBetween(2, 20);
        final List<IndexMetadata> indices = new ArrayList<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final String uuid = randomUUID();
            final IndexMetadata indexMetadata = IndexMetadata.builder(org.elasticsearch.core.Strings.format("index-%02d", i))
                .settings(
                    indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                )
                .build();
            indices.add(indexMetadata);
            projectBuilder.put(indexMetadata, false);
        }

        final Metadata.ProjectLookup lookup = Metadata.builder().put(projectBuilder).build().getProjectLookup();
        assertThat(lookup, Matchers.instanceOf(Metadata.SingleProjectLookup.class));
        indices.forEach(im -> {
            final Index index = im.getIndex();
            assertThat(lookup.project(index).map(ProjectMetadata::id), isPresentWith(projectId));

            Index alt1 = new Index(index.getName(), randomValueOtherThan(im.getIndexUUID(), ESTestCase::randomUUID));
            assertThat(lookup.project(alt1), isEmpty());

            Index alt2 = new Index(randomAlphaOfLength(8), im.getIndexUUID());
            assertThat(lookup.project(alt2), isEmpty());
        });
    }

    public void testProjectLookupWithMultipleProjects() {
        final int numberOfProjects = randomIntBetween(2, 8);
        final Metadata.Builder metadataBuilder = Metadata.builder();
        final Map<ProjectId, List<IndexMetadata>> indices = Maps.newMapWithExpectedSize(numberOfProjects);
        for (int p = 1; p <= numberOfProjects; p++) {
            final ProjectId projectId = ProjectId.fromId(org.elasticsearch.core.Strings.format("proj_%02d", p));
            final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

            final int numberOfIndices = randomIntBetween(p, 10);
            indices.put(projectId, new ArrayList<>(numberOfIndices));
            for (int i = 0; i < numberOfIndices; i++) {
                final String uuid = randomUUID();
                final IndexMetadata indexMetadata = IndexMetadata.builder(org.elasticsearch.core.Strings.format("index-%02d", i))
                    .settings(
                        indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    )
                    .build();
                indices.get(projectId).add(indexMetadata);
                projectBuilder.put(indexMetadata, false);
            }
            metadataBuilder.put(projectBuilder);
        }

        final Metadata.ProjectLookup lookup = metadataBuilder.build().getProjectLookup();
        assertThat(lookup, Matchers.instanceOf(Metadata.MultiProjectLookup.class));

        indices.forEach((project, ix) -> ix.forEach(im -> {
            final Index index = im.getIndex();
            assertThat(lookup.project(index).map(ProjectMetadata::id), isPresentWith(project));

            Index alt1 = new Index(index.getName(), randomValueOtherThan(im.getIndexUUID(), ESTestCase::randomUUID));
            assertThat(lookup.project(alt1), isEmpty());

            Index alt2 = new Index(randomAlphaOfLength(8), im.getIndexUUID());
            assertThat(lookup.project(alt2), isEmpty());
        }));
    }

    public void testOldestIndexVersionAllProjects() {
        final int numProjects = between(1, 5);
        final List<IndexVersion> indexVersions = randomList(
            numProjects,
            numProjects,
            () -> IndexVersionUtils.randomCompatibleVersion(random())
        );

        final Map<ProjectId, ProjectMetadata> projectMetadataMap = new HashMap<>();
        for (int i = 0; i < numProjects; i++) {
            final var projectId = i == 0 ? Metadata.DEFAULT_PROJECT_ID : randomUniqueProjectId();
            projectMetadataMap.put(
                projectId,
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(randomIdentifier()).settings(indexSettings(indexVersions.get(i), 1, 1)))
                    .build()
            );
        }
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA).projectMetadata(projectMetadataMap).build();

        assertThat(metadata.oldestIndexVersionAllProjects(), equalTo(indexVersions.stream().min(Comparator.naturalOrder()).orElseThrow()));
    }

    public static Metadata randomMetadata() {
        return randomMetadata(1);
    }

    public static Metadata randomMetadata(int numDataStreams) {
        final ProjectMetadata project = randomProject(Metadata.DEFAULT_PROJECT_ID, numDataStreams);
        return randomMetadata(List.of(project));
    }

    public static Metadata randomMetadata(Collection<ProjectMetadata> projects) {
        final Metadata.Builder md = Metadata.builder()
            .persistentSettings(Settings.builder().put("setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .transientSettings(Settings.builder().put("other_setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .clusterUUID("uuid" + randomAlphaOfLength(3))
            .clusterUUIDCommitted(randomBoolean())
            .version(randomNonNegativeLong());
        for (var project : projects) {
            md.put(project);
        }
        return md.build();
    }

    public static ProjectMetadata randomProject(ProjectId id, int numDataStreams) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(id)
            .put(buildIndexMetadata("index", "alias", randomBoolean() ? null : randomBoolean()).build(), randomBoolean())
            .put(
                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                    .build()
            )
            .indexGraveyard(IndexGraveyardTests.createRandom())
            .put("component_template_" + randomAlphaOfLength(3), ComponentTemplateTests.randomInstance())
            .put("index_template_v2_" + randomAlphaOfLength(3), ComposableIndexTemplateTests.randomInstance());

        for (int k = 0; k < numDataStreams; k++) {
            DataStream randomDataStream = DataStreamTestHelper.randomInstance();
            for (Index index : randomDataStream.getIndices()) {
                project.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
            }
            project.put(randomDataStream);
        }
        return project.build();
    }

    private static ToXContent.Params formatParams() {
        return new ToXContent.MapParams(Map.of("binary", "true", Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
    }

    private static class TestClusterCustomMetadata implements Metadata.ClusterCustom {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        @Override
        public Diff<Metadata.ClusterCustom> diff(Metadata.ClusterCustom previousState) {
            return null;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return null;
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
