/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodes;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodesWithRandomSettings;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesSettingsValidatorTests extends ESTestCase {

    public void testSettingsValidation() {
        final Set<Setting<?>> availableSettings = Set.of(
            Setting.intSetting("test.invalid_value", 1, Setting.Property.NodeScope),
            Setting.intSetting("test.invalid_range", 1, 1, 100, Setting.Property.NodeScope),
            NODE_EXTERNAL_ID_SETTING,
            NODE_NAME_SETTING
        );
        final Consumer<Settings.Builder> settingsProvider = settings -> {
            if (randomBoolean()) {
                settings.put("test.invalid_value", randomAlphaOfLength(10));
            } else {
                settings.put("test.invalid_range", randomFrom(-1, Integer.MAX_VALUE));
            }
        };

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, availableSettings);
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);

        final DesiredNodes desiredNodes = randomDesiredNodes(Version.CURRENT, settingsProvider);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.validate(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("Failed to parse value"));
    }

    public void testNodeVersionValidation() {
        final DesiredNodes desiredNodes = new DesiredNodes(
            randomAlphaOfLength(10),
            randomIntBetween(1, 20),
            List.of(randomDesiredNode(Version.CURRENT.previousMajor(), (settings) -> {}))
        );

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.emptySet());
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.validate(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("Illegal node version"));
    }

    public void testUnknownSettingsInKnownVersionsAreInvalid() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.emptySet());
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);
        final DesiredNodes desiredNodes = randomDesiredNodes();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.validate(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("unknown setting"));
    }

    public void testSettingsInFutureVersionsAreNotValidated() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.emptySet());
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);

        final DesiredNodes desiredNodes = randomDesiredNodesWithRandomSettings(Version.fromString("99.9.0"));
        validator.validate(desiredNodes);
    }

    public void testExternalIDIsRequired() {
        final Set<Setting<?>> availableSettings = Set.of(
            Setting.intSetting("test.value", 1, Setting.Property.NodeScope),
            NODE_EXTERNAL_ID_SETTING,
            NODE_NAME_SETTING
        );
        final Consumer<Settings.Builder> settingsProvider = settings -> {
            settings.put("test.value", randomInt());
            settings.remove(NODE_EXTERNAL_ID_SETTING.getKey());
            settings.remove(NODE_NAME_SETTING.getKey());
        };

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, availableSettings);
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);

        final DesiredNodes desiredNodes = randomDesiredNodes(Version.CURRENT, settingsProvider);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.validate(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("[node.name] or [node.external_id] is missing or empty"));
    }

    public void testNodeProcessorsValidation() {
        final Set<Setting<?>> availableSettings = Set.of(NODE_PROCESSORS_SETTING, NODE_EXTERNAL_ID_SETTING, NODE_NAME_SETTING);

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, availableSettings);
        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(clusterSettings);

        {
            int desiredNodeProcessors = 128;
            Settings nodeSettings = Settings.builder()
                .put(NODE_EXTERNAL_ID_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_PROCESSORS_SETTING.getKey(), desiredNodeProcessors)
                .build();
            final DesiredNodes desiredNodes = new DesiredNodes(
                randomAlphaOfLength(10),
                1,
                List.of(new DesiredNode(nodeSettings, desiredNodeProcessors, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT))
            );

            validator.validate(desiredNodes);
        }

        {
            int desiredNodeProcessors = 128;
            Settings nodeSettings = Settings.builder()
                .put(NODE_EXTERNAL_ID_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_PROCESSORS_SETTING.getKey(), desiredNodeProcessors + 1)
                .build();
            final DesiredNodes desiredNodes = new DesiredNodes(
                randomAlphaOfLength(10),
                1,
                List.of(new DesiredNode(nodeSettings, desiredNodeProcessors, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT))
            );

            final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.validate(desiredNodes));
            assertThat(exception.getMessage(), containsString("Nodes with ids"));
            assertThat(exception.getMessage(), containsString("contain invalid settings"));
            assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
            assertThat(
                exception.getSuppressed()[0].getMessage(),
                containsString("Failed to parse value [129] for setting [node.processors] must be <= 128")
            );
        }

    }
}
