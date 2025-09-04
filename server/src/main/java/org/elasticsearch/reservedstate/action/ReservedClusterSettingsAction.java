/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsUpdater;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This Action is the reserved state save version of RestClusterUpdateSettingsAction
 * <p>
 * It is used by the ReservedClusterStateService to update the persistent cluster settings.
 * Since transient cluster settings are deprecated, this action doesn't support updating transient cluster settings.
 */
public class ReservedClusterSettingsAction implements ReservedClusterStateHandler<Map<String, Object>> {

    private static final Logger logger = LogManager.getLogger(ReservedClusterSettingsAction.class);

    public static final String NAME = "cluster_settings";

    private final ClusterSettings clusterSettings;

    public ReservedClusterSettingsAction(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    private static ClusterUpdateSettingsRequest prepare(Object input, Set<String> previouslySet) {
        // load the new settings into a builder so their paths are normalized
        @SuppressWarnings("unchecked")
        Settings.Builder newSettings = Settings.builder().loadFromMap((Map<String, ?>) input);

        // now the new and old settings can be compared to find which are missing for deletion
        Set<String> toDelete = new HashSet<>(previouslySet);
        toDelete.removeAll(newSettings.keys());
        toDelete.forEach(k -> newSettings.put(k, (String) null));

        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest(
            RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
            RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT
        );
        clusterUpdateSettingsRequest.persistentSettings(newSettings);
        return clusterUpdateSettingsRequest;
    }

    @Override
    public TransformState transform(Map<String, Object> input, TransformState prevState) {
        ClusterUpdateSettingsRequest request = prepare(input, prevState.keys());

        // allow empty requests, this is how we clean up settings
        if (request.persistentSettings().isEmpty() == false) {
            validate(request);
        }

        final var state = new SettingsUpdater(clusterSettings).updateSettings(
            prevState.state(),
            request.transientSettings(),
            request.persistentSettings(),
            logger
        );

        Set<String> currentKeys = request.persistentSettings()
            .keySet()
            .stream()
            .filter(k -> request.persistentSettings().hasValue(k))
            .collect(Collectors.toSet());

        return new TransformState(state, currentKeys);
    }

    @Override
    public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
        return parser.map();
    }
}
