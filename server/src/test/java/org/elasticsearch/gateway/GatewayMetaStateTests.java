/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GatewayMetaStateTests extends ESTestCase {

    public void testUpdateTemplateMetadataOnUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(templates -> {
            templates.put(
                "added_test_template",
                IndexTemplateMetadata.builder("added_test_template").patterns(randomIndexPatterns()).build()
            );
            return templates;
        }));

        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetadataUpgrade() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testIndexMetadataUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(true), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertFalse(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataNoChange() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(HashMap::new));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testIndexTemplateValidation() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(customs -> {
            throw new IllegalStateException("template is incompatible");
        }));
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

        }));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockIndexMetadataVerifier(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
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
                    () -> gatewayMetaState.start(null, null, null, null, null, null, null, List.of(duplicatePlugin, duplicatePlugin))
                ).getMessage(),
                containsString("multiple persisted-state factories")
            );

            gatewayMetaState.start(null, null, null, null, null, null, null, List.of(new ClusterCoordinationPlugin() {
                @Override
                public Optional<PersistedStateFactory> getPersistedStateFactory() {
                    return Optional.of((settings, transportService, persistedClusterStateService) -> testPersistedState);
                }
            }));
            assertSame(testPersistedState, gatewayMetaState.getPersistedState());
        }
    }

    private static class MockIndexMetadataVerifier extends IndexMetadataVerifier {
        private final boolean upgrade;

        MockIndexMetadataVerifier(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata verifyIndexMetadata(IndexMetadata indexMetadata, IndexVersion minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetadata.builder(indexMetadata).build() : indexMetadata;
        }
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
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

    private static Metadata randomMetadata(TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static Metadata randomMetadataWithIndexTemplates(String... templates) {
        Metadata.Builder builder = Metadata.builder();
        for (String template : templates) {
            IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.builder(template)
                .settings(indexSettings(Version.CURRENT, randomIntBetween(1, 5), randomIntBetween(0, 3)))
                .patterns(randomIndexPatterns())
                .build();
            builder.put(templateMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
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
