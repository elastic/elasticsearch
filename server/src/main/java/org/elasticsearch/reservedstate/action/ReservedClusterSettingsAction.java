/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SettingsUpdater;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
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
    private ClusterUpdateSettingsRequest prepare(Object input, Set<String> previouslySet) {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();

        Map<String, Object> persistentSettings = new HashMap<>();
        Set<String> toDelete = new HashSet<>(previouslySet);

        Map<String, Object> settings = (Map<String, Object>) input;

        settings.forEach((k, v) -> {
            persistentSettings.put(k, v);
            toDelete.remove(k);
        });

        toDelete.forEach(k -> persistentSettings.put(k, null));

        clusterUpdateSettingsRequest.persistentSettings(persistentSettings);
        return clusterUpdateSettingsRequest;
    }

    @Override
    public TransformState transform(Object input, TransformState prevState) {
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
