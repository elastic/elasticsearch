/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.immutablestate.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.immutablestate.TransformState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.Maps.asMap;

/**
 * This Action is the immutable state save version of RestClusterUpdateSettingsAction
 * <p>
 * It is used by the ImmutableClusterStateController to update the persistent cluster settings.
 * Since transient cluster settings are deprecated, this action doesn't support updating transient cluster settings.
 */
public class ImmutableClusterSettingsAction implements ImmutableClusterStateHandler<ClusterUpdateSettingsRequest> {

    public static final String NAME = "cluster_settings";

    private final ClusterSettings clusterSettings;

    public ImmutableClusterSettingsAction(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public String name() {
        return NAME;
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

        // allow empty requests, this is how we clean up settings
        if (request.persistentSettings().isEmpty() == false) {
            validate(request);
        }

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
