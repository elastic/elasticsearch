/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestClusterCustomMetadata;
import org.elasticsearch.test.TestProjectCustomMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GatewayMetaStateTests extends ESTestCase {

    private ProjectId projectId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
    }

    public void testUpdateTemplateMetadataOnUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(templates -> {
            templates.put(
                "added_test_template",
                IndexTemplateMetadata.builder("added_test_template").patterns(randomIndexPatterns()).build()
            );
            return templates;
        }), List.of());

        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertTrue(upgrade.hasProject(projectId));
        upgrade.projects()
            .forEach((projectId, projectMetadata) -> assertTrue(projectMetadata.templates().containsKey("added_test_template")));
    }

    public void testNoMetadataUpgrade() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList(), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade.getProject(projectId)) {
            assertTrue(metadata.getProject(projectId).hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList(), List.of());
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testIndexMetadataUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList(), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(true), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade.getProject(projectId)) {
            assertFalse(metadata.getProject(projectId).hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataNoChange() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(HashMap::new), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade.getProject(projectId)) {
            assertTrue(metadata.getProject(projectId).hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadata_appliesUpgraders() {
        ProjectCustomMetadata2 custom2 = new ProjectCustomMetadata2("some data");
        // Test with a ProjectCustomMetadata1 and a ProjectCustomMetadata2...
        Metadata originalMetadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .put(
                ProjectMetadata.builder(projectId)
                    .putCustom(ProjectCustomMetadata1.TYPE, new ProjectCustomMetadata1("data"))
                    .putCustom(ProjectCustomMetadata2.TYPE, custom2)
            )
            .build();
        // ...and two sets of upgraders which affect ProjectCustomMetadata1 and some other types...
        Map<String, UnaryOperator<Metadata.ProjectCustom>> customUpgraders = Map.of(
            ProjectCustomMetadata1.TYPE,
            toUpgrade -> new ProjectCustomMetadata1("new " + ((ProjectCustomMetadata1) toUpgrade).getData()),
            "not_" + ProjectCustomMetadata1.TYPE,
            toUpgrade -> {
                fail("This upgrader should not be invoked");
                return toUpgrade;
            }
        );
        Map<String, UnaryOperator<Metadata.ProjectCustom>> moreCustomUpgraders = Map.of(
            "also_not_" + ProjectCustomMetadata1.TYPE,
            toUpgrade -> {
                fail("This upgrader should not be invoked");
                return toUpgrade;
            }
        );
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(HashMap::new), List.of(customUpgraders, moreCustomUpgraders));
        Metadata upgradedMetadata = GatewayMetaState.upgradeMetadata(
            originalMetadata,
            new MockIndexMetadataVerifier(false),
            metadataUpgrader
        );
        // ...and assert that the ProjectCustomMetadata1 has been upgraded...
        assertEquals(new ProjectCustomMetadata1("new data"), upgradedMetadata.getProject(projectId).custom(ProjectCustomMetadata1.TYPE));
        // ...but the ProjectCustomMetadata2 is untouched.
        assertSame(custom2, upgradedMetadata.getProject(projectId).custom(ProjectCustomMetadata2.TYPE));
    }

    public void testCustomMetadata_appliesMultipleUpgraders() {
        // Test with a ProjectCustomMetadata1 and a ProjectCustomMetadata2...
        Metadata originalMetadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .put(
                ProjectMetadata.builder(projectId)
                    .putCustom(ProjectCustomMetadata1.TYPE, new ProjectCustomMetadata1("data"))
                    .putCustom(ProjectCustomMetadata2.TYPE, new ProjectCustomMetadata2("other data"))
            )
            .build();
        // ...and a set of upgraders which affects both of those...
        Map<String, UnaryOperator<Metadata.ProjectCustom>> customUpgraders = Map.of(
            ProjectCustomMetadata1.TYPE,
            toUpgrade -> new ProjectCustomMetadata1("new " + ((ProjectCustomMetadata1) toUpgrade).getData()),
            ProjectCustomMetadata2.TYPE,
            toUpgrade -> new ProjectCustomMetadata2("new " + ((ProjectCustomMetadata2) toUpgrade).getData())
        );
        // ...and another set of upgraders which applies a second upgrade to ProjectCustomMetadata2...
        Map<String, UnaryOperator<Metadata.ProjectCustom>> moreCustomUpgraders = Map.of(
            ProjectCustomMetadata2.TYPE,
            toUpgrade -> new ProjectCustomMetadata2("more " + ((ProjectCustomMetadata2) toUpgrade).getData())
        );
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(List.of(HashMap::new), List.of(customUpgraders, moreCustomUpgraders));
        Metadata upgradedMetadata = GatewayMetaState.upgradeMetadata(
            originalMetadata,
            new MockIndexMetadataVerifier(false),
            metadataUpgrader
        );
        // ...and assert that the first upgrader has been applied to the ProjectCustomMetadata1...
        assertEquals(new ProjectCustomMetadata1("new data"), upgradedMetadata.getProject(projectId).custom(ProjectCustomMetadata1.TYPE));
        // ...and both upgraders have been applied to the ProjectCustomMetadata2.
        assertEquals(
            new ProjectCustomMetadata2("more new other data"),
            upgradedMetadata.getProject(projectId).custom(ProjectCustomMetadata2.TYPE)
        );
    }

    public void testIndexTemplateValidation() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(customs -> {
            throw new IllegalStateException("template is incompatible");
        }), List.of());
        String message = expectThrows(
            IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader)
        ).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }

    public void testMultipleIndexTemplateUpgrade() {
        final Metadata metadata = switch (randomIntBetween(0, 2)) {
            case 0 -> randomMetadataWithIndexTemplates("template1", "template2");
            case 1 -> randomMetadataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
            case 2 -> randomMetadata();
            default -> throw new IllegalStateException("should never happen");
        };
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Arrays.asList(indexTemplateMetadatas -> {
            indexTemplateMetadatas.put(
                "template1",
                IndexTemplateMetadata.builder("template1")
                    .patterns(randomIndexPatterns())
                    .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                    .build()
            );
            return indexTemplateMetadatas;

        }, indexTemplateMetadatas -> {
            indexTemplateMetadatas.put(
                "template2",
                IndexTemplateMetadata.builder("template2")
                    .patterns(randomIndexPatterns())
                    .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build())
                    .build()
            );
            return indexTemplateMetadatas;
        }), List.of());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.getProject(projectId).templates().get("template1"));
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.getProject(projectId).templates().get("template1").settings()),
            equalTo(20)
        );
        assertNotNull(upgrade.getProject(projectId).templates().get("template2"));
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.getProject(projectId).templates().get("template2").settings()),
            equalTo(10)
        );
        for (IndexMetadata indexMetadata : upgrade.getProject(projectId)) {
            assertTrue(metadata.getProject(projectId).hasIndexMetadata(indexMetadata));
        }
    }

    public void testPluggablePersistedStateValidation() throws IOException {
        try (
            var gatewayMetaState = new GatewayMetaState();
            var testPersistedState = new InMemoryPersistedState(0, ClusterState.EMPTY_STATE)
        ) {
            final var duplicatePlugin = new ClusterCoordinationPlugin() {
                @Override
                public Optional<PersistedStateFactory> getPersistedStateFactory() {
                    return Optional.of(
                        (settings, transportService, persistedClusterStateService) -> { throw new AssertionError("should not be called"); }
                    );
                }
            };
            assertThat(
                expectThrows(
                    IllegalStateException.class,
                    () -> gatewayMetaState.start(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        List.of(duplicatePlugin, duplicatePlugin),
                        CompatibilityVersionsUtils.staticCurrent()
                    )
                ).getMessage(),
                containsString("multiple persisted-state factories")
            );

            gatewayMetaState.start(null, null, null, null, null, null, null, List.of(new ClusterCoordinationPlugin() {
                @Override
                public Optional<PersistedStateFactory> getPersistedStateFactory() {
                    return Optional.of((settings, transportService, persistedClusterStateService) -> testPersistedState);
                }
            }), CompatibilityVersionsUtils.staticCurrent());
            assertSame(testPersistedState, gatewayMetaState.getPersistedState());
        }
    }

    private static class MockIndexMetadataVerifier extends IndexMetadataVerifier {
        private final boolean upgrade;

        MockIndexMetadataVerifier(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null, null, MapperMetrics.NOOP);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata verifyIndexMetadata(
            IndexMetadata indexMetadata,
            IndexVersion minimumIndexCompatibilityVersion,
            IndexVersion minimumReadOnlyIndexCompatibilityVersion
        ) {
            return upgrade ? IndexMetadata.builder(indexMetadata).build() : indexMetadata;
        }
    }

    private static class CustomMetadata1 extends TestClusterCustomMetadata {
        public static final String TYPE = "custom_md_1";

        CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class ProjectCustomMetadata1 extends TestProjectCustomMetadata {
        public static final String TYPE = "custom_project_md_1";

        ProjectCustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class ProjectCustomMetadata2 extends TestProjectCustomMetadata {
        public static final String TYPE = "custom_project_md_2";

        ProjectCustomMetadata2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private Metadata randomMetadata(TestClusterCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder(Metadata.EMPTY_METADATA);
        builder.put(ProjectMetadata.builder(projectId));
        for (TestClusterCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.getProject(projectId)
                .put(
                    IndexMetadata.builder(randomAlphaOfLength(10))
                        .settings(settings(IndexVersion.current()))
                        .numberOfReplicas(randomIntBetween(0, 3))
                        .numberOfShards(randomIntBetween(1, 5))
                );
        }
        return builder.build();
    }

    private Metadata randomMetadataWithIndexTemplates(String... templates) {
        Metadata.Builder builder = Metadata.builder(Metadata.EMPTY_METADATA);
        builder.put(ProjectMetadata.builder(projectId));
        for (String template : templates) {
            IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.builder(template)
                .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 3)))
                .patterns(randomIndexPatterns())
                .build();
            builder.getProject(projectId).put(templateMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.getProject(projectId)
                .put(
                    IndexMetadata.builder(randomAlphaOfLength(10))
                        .settings(settings(IndexVersion.current()))
                        .numberOfReplicas(randomIntBetween(0, 3))
                        .numberOfShards(randomIntBetween(1, 5))
                );
        }
        return builder.build();
    }

    private static List<String> randomIndexPatterns() {
        return Arrays.asList(Objects.requireNonNull(generateRandomStringArray(10, 100, false, false)));
    }
}
