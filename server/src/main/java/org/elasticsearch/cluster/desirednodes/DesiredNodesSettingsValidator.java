/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.lang.String.format;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.createNodeProcessorsSetting;

public class DesiredNodesSettingsValidator {
    private final ClusterSettings clusterSettings;

    public DesiredNodesSettingsValidator(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public void validate(DesiredNodes desiredNodes) {
        final List<RuntimeException> exceptions = new ArrayList<>();
        for (DesiredNode node : desiredNodes.nodes()) {
            try {
                validate(node);
            } catch (RuntimeException e) {
                // TODO: add nodeID
                exceptions.add(e);
            }
        }

        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    private void validate(DesiredNode node) {
        if (node.externalID() == null || node.externalID().isEmpty()) {
            throw new IllegalArgumentException("External id missing");
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
                format(Locale.ROOT, "Node [%s] has unknown settings %s", node.externalID(), unknownSettings)
            );
        }

        final Settings updatedSettings = unknownSettings.isEmpty() ? settings : updatedSettingsBuilder.build();
        final ClusterSettings updatedClusterSettings = hasOverriddenSettings
            ? clusterSettings
            : new ClusterSettings(updatedSettings, settingSet);
        updatedClusterSettings.validate(updatedSettings, true);
    }

    protected Setting<?> maybeOverride(DesiredNode node, Setting<?> setting) {
        if (NODE_PROCESSORS_SETTING.getKey().equals(setting.getKey())) {
            return createNodeProcessorsSetting(node.processors());
        }
        return setting;
    }
}
