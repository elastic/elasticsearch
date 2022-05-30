/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.operator.TransformState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO: Add docs
 */
public class OperatorClusterUpdateSettingsAction implements OperatorHandler<ClusterUpdateSettingsRequest> {

    public static final String KEY = "cluster_settings";

    private final ClusterSettings clusterSettings;

    public OperatorClusterUpdateSettingsAction(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public String key() {
        return KEY;
    }

    @SuppressWarnings("unchecked")
    private ClusterUpdateSettingsRequest prepare(Object input, Set<String> previouslySet) {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();

        Map<String, ?> source = asMap(input);
        Map<String, Object> persistentSettings = new HashMap<>();
        Set<String> toDelete = new HashSet<>(previouslySet);

        source.forEach((k, v) -> {
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
        validate(request);

        ClusterState state = prevState.state();

        TransportClusterUpdateSettingsAction.ClusterUpdateSettingsTask updateSettingsTask =
            new TransportClusterUpdateSettingsAction.ClusterUpdateSettingsTask(clusterSettings, request);

        state = updateSettingsTask.execute(state);
        Set<String> currentKeys = request.persistentSettings()
            .keySet()
            .stream()
            .filter(k -> request.persistentSettings().hasValue(k))
            .collect(Collectors.toSet());

        return new TransformState(state, currentKeys);
    }
}
