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
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class DesiredNodesSettingsValidator {
    private record DesiredNodeValidationError(int position, @Nullable String externalId, RuntimeException exception) {
        public String externalId() {
            return externalId == null ? "<missing>" : externalId;
        }
    }

    private final ClusterSettings clusterSettings;

    public DesiredNodesSettingsValidator(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public void validate(DesiredNodes desiredNodes) {
        final List<DesiredNodeValidationError> validationErrors = new ArrayList<>();
        final List<DesiredNode> nodes = desiredNodes.nodes();
        for (int i = 0; i < nodes.size(); i++) {
            final DesiredNode node = nodes.get(i);
            try {
                validate(node);
            } catch (IllegalArgumentException e) {
                validationErrors.add(new DesiredNodeValidationError(i, node.externalId(), e));
            }
        }

        if (validationErrors.isEmpty() == false) {
            final String nodeIndicesWithFailures = validationErrors.stream()
                .map(DesiredNodeValidationError::position)
                .map(i -> Integer.toString(i))
                .collect(Collectors.joining(","));

            final String nodeIdsWithFailures = validationErrors.stream()
                .map(DesiredNodeValidationError::externalId)
                .collect(Collectors.joining(","));
            IllegalArgumentException invalidSettingsException = new IllegalArgumentException(
                format(
                    Locale.ROOT,
                    "Nodes with ids [%s] in positions [%s] contain invalid settings",
                    nodeIdsWithFailures,
                    nodeIndicesWithFailures
                )
            );
            for (DesiredNodeValidationError exceptionTuple : validationErrors) {
                invalidSettingsException.addSuppressed(exceptionTuple.exception);
            }
            throw invalidSettingsException;
        }
    }

    private void validate(DesiredNode node) {
        if (node.version().before(Version.CURRENT)) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "Illegal node version [%s]. Only [%s] or newer versions are supported", node.version(), Version.CURRENT)
            );
        }

        if (node.externalId() == null) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "[%s] or [%s] is missing or empty", NODE_NAME_SETTING.getKey(), NODE_EXTERNAL_ID_SETTING.getKey())
            );
        }

        // Validating settings for future versions can be unsafe:
        // - If the legal range is upgraded in the newer version
        // - If a new setting is used as the default value for a previous setting
        // To avoid considering these as invalid settings,
        // We just don't validate settings for versions in newer versions.
        if (node.version().after(Version.CURRENT)) {
            return;
        }

        Settings settings = node.settings();

        // node.processors rely on the environment to define its ranges, in this case
        // we create a new setting just to run the validations using the desired node
        // number of available processors
        if (settings.hasValue(NODE_PROCESSORS_SETTING.getKey())) {
            Setting.intSetting(NODE_PROCESSORS_SETTING.getKey(), node.processors(), 1, node.processors(), Setting.Property.NodeScope)
                .get(settings);
            final Settings.Builder updatedSettings = Settings.builder().put(settings);
            updatedSettings.remove(NODE_PROCESSORS_SETTING.getKey());
            settings = updatedSettings.build();
        }

        clusterSettings.validate(settings, true);
    }

}
