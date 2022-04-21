/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.operator.OperatorHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction.PERSISTENT;
import static org.elasticsearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction.TRANSIENT;

/**
 * TODO: Add docs
 */
public class OperatorClusterSettingsAction implements OperatorHandler<ClusterUpdateSettingsRequest> {

    public static final String KEY = "cluster";

    @Override
    public String key() {
        return KEY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ClusterUpdateSettingsRequest> prepare(Object input) {
        if (input instanceof Map<?, ?> source) {
            final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();

            if (source.containsKey(PERSISTENT)) {
                clusterUpdateSettingsRequest.persistentSettings((Map<String, ?>) source.get(PERSISTENT));
            }

            return List.of(clusterUpdateSettingsRequest);
        }
        throw new IllegalStateException("Unsupported " + KEY + " request format");
    }

    @Override
    public Optional<ClusterState> transformClusterState(
        Collection<ClusterUpdateSettingsRequest> requests,
        ClusterState.Builder clusterStateBuilder,
        ClusterState previous) {
        return Optional.empty();
    }
}
