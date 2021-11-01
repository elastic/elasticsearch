/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestClusterUpdateSettingsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterUpdateSettingsAction.class);
    static final String TRANSIENT_SETTINGS_DEPRECATION_MESSAGE = "[transient settings removal] Updating cluster settings "
        + "through transient settings is deprecated. Some Elastic products may make use of transient settings and those "
        + "should not be changed. Any custom use of transient settings should be replaced by persistent settings.";

    private static final String PERSISTENT = "persistent";
    private static final String TRANSIENT = "transient";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_cluster/settings"));
    }

    @Override
    public String getName() {
        return "cluster_update_settings_action";
    }

    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(
            request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout())
        );
        Map<String, Object> source;
        try (XContentParser parser = request.contentParser()) {
            source = parser.map();
        }
        if (source.containsKey(TRANSIENT)) {
            Map<String, ?> transientSettings = (Map<String, ?>) source.get(TRANSIENT);

            // We check for empty transient settings map because ClusterUpdateSettingsRequest initializes
            // each of the settings to an empty collection. When the RestClient is used, we'll get an empty
            // transient settings map, even if we never set any transient settings.
            if (transientSettings.isEmpty() == false) {
                deprecationLogger.warn(DeprecationCategory.SETTINGS, "transient_settings", TRANSIENT_SETTINGS_DEPRECATION_MESSAGE);
            }
            clusterUpdateSettingsRequest.transientSettings(transientSettings);
        }
        if (source.containsKey(PERSISTENT)) {
            clusterUpdateSettingsRequest.persistentSettings((Map<String, ?>) source.get(PERSISTENT));
        }

        return channel -> client.admin().cluster().updateSettings(clusterUpdateSettingsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
