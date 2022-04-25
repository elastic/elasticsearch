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

import java.util.Map;

import static org.elasticsearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction.PERSISTENT;

/**
 * TODO: Add docs
 */
public class OperatorClusterUpdateSettingsAction implements OperatorHandler<ClusterUpdateSettingsRequest> {

    public static final String KEY = "cluster";

    private final ClusterSettings clusterSettings;

    public OperatorClusterUpdateSettingsAction(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public String key() {
        return KEY;
    }

    @SuppressWarnings("unchecked")
    private ClusterUpdateSettingsRequest prepare(Object input) {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();

        Map<String, ?> source = asMap(input);

        if (source.containsKey(PERSISTENT)) {
            clusterUpdateSettingsRequest.persistentSettings((Map<String, ?>) source.get(PERSISTENT));
        }

        return clusterUpdateSettingsRequest;
    }

    @Override
    public ClusterState transform(Object input, ClusterState state) {

        ClusterUpdateSettingsRequest request = prepare(input);
        validate(request);

        TransportClusterUpdateSettingsAction.ClusterUpdateSettingsTask updateSettingsTask =
            new TransportClusterUpdateSettingsAction.ClusterUpdateSettingsTask(clusterSettings, request);
        return updateSettingsTask.execute(state);
    }
}
