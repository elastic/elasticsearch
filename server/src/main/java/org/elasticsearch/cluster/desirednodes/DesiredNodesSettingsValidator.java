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

public class DesiredNodesSettingsValidator {
    private record DesiredNodeValidationError(int position, @Nullable String externalId, RuntimeException exception) {}

    private final ClusterSettings clusterSettings;

    public DesiredNodesSettingsValidator(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public void validate(List<DesiredNode> nodes) {
        final List<DesiredNodeValidationError> validationErrors = new ArrayList<>();
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
                    "Nodes with ids [%s] in positions [%s] contain invalid settings: [%s]",
                    nodeIdsWithFailures,
                    nodeIndicesWithFailures,
                    validationErrors.stream()
                        .map(error -> format(Locale.ROOT, "'%s': '%s'", error.externalId, error.exception.getMessage()))
                        .collect(Collectors.joining(", "))
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
            int minProcessors = node.roundedDownMinProcessors();
            Integer roundedUpMaxProcessors = node.roundedUpMaxProcessors();
            int maxProcessors = roundedUpMaxProcessors == null ? minProcessors : roundedUpMaxProcessors;
            Setting.doubleSetting(
                NODE_PROCESSORS_SETTING.getKey(),
                minProcessors,
                Double.MIN_VALUE,
                maxProcessors,
                Setting.Property.NodeScope
            ).get(settings);
            final Settings.Builder updatedSettings = Settings.builder().put(settings);
            updatedSettings.remove(NODE_PROCESSORS_SETTING.getKey());
            settings = updatedSettings.build();
        }

        clusterSettings.validate(settings, true);
    }

}
