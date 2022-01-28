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
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.createNodeProcessorsSetting;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class DesiredNodesSettingsValidator {
    private final ClusterSettings clusterSettings;

    public DesiredNodesSettingsValidator(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public void validate(DesiredNodes desiredNodes) {
        final List<Tuple<Integer, RuntimeException>> exceptions = new ArrayList<>();
        final List<DesiredNode> nodes = desiredNodes.nodes();
        for (int i = 0; i < nodes.size(); i++) {
            final DesiredNode node = nodes.get(i);
            try {
                validate(node);
            } catch (RuntimeException e) {
                exceptions.add(Tuple.tuple(i, e));
            }
        }

        if (exceptions.isEmpty() == false) {
            final String nodeIndicesWithFailures = exceptions.stream()
                .map(Tuple::v1)
                .map(i -> Integer.toString(i))
                .collect(Collectors.joining(","));
            IllegalArgumentException invalidSettingsException = new IllegalArgumentException(
                format(Locale.ROOT, "Nodes in positions [%s] contain invalid settings", nodeIndicesWithFailures)
            );
            for (Tuple<Integer, RuntimeException> exceptionTuple : exceptions) {
                invalidSettingsException.addSuppressed(exceptionTuple.v2());
            }
            throw invalidSettingsException;
        }
    }

    private void validate(DesiredNode node) {
        if (node.externalId() == null) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "[%s] or [%s] is missing or empty", NODE_NAME_SETTING.getKey(), NODE_EXTERNAL_ID_SETTING.getKey())
            );
        }

        final Settings settings = node.settings();

        final Set<Setting<?>> settingSet = new HashSet<>();
        final List<String> unknownSettings = new ArrayList<>();
        // If we're dealing with a node in a newer version it's possible
        // that this node doesn't know about these. In that case we do a best
        // effort and try to validate the settings that are known to this version.
        final Settings.Builder updatedSettingsBuilder = Settings.builder().put(settings);
        boolean hasOverriddenSettings = false;
        for (String settingName : settings.keySet()) {
            Setting<?> setting = clusterSettings.get(settingName);
            if (setting == null) {
                unknownSettings.add(settingName);
                updatedSettingsBuilder.remove(settingName);
            } else {
                // Some settings might be defined using environmental information
                // such as the number of available processors to define valid setting
                // value ranges, in that case we should define new settings that take
                // into account the node properties that we're evaluating.
                Setting<?> maybeOverrideSetting = maybeOverride(node, setting);
                hasOverriddenSettings |= (setting != maybeOverrideSetting);
                settingSet.add(maybeOverrideSetting);
            }
        }

        if (unknownSettings.isEmpty() == false && node.version().onOrBefore(Version.CURRENT)) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "Node [%s] has unknown settings %s", node.externalId(), unknownSettings)
            );
        }

        final Settings updatedSettings = unknownSettings.isEmpty() ? settings : updatedSettingsBuilder.build();
        final ClusterSettings updatedClusterSettings = hasOverriddenSettings
            ? new ClusterSettings(updatedSettings, settingSet)
            : clusterSettings;
        updatedClusterSettings.validate(updatedSettings, true);
    }

    protected Setting<?> maybeOverride(DesiredNode node, Setting<?> setting) {
        if (NODE_PROCESSORS_SETTING.getKey().equals(setting.getKey())) {
            return createNodeProcessorsSetting(node.processors());
        }
        return setting;
    }

}
