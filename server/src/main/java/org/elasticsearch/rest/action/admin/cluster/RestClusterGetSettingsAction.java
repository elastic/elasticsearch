/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterGetSettingsAction extends BaseRestHandler {

    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public RestClusterGetSettingsAction(Settings settings, ClusterSettings clusterSettings, SettingsFilter settingsFilter) {
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/settings"));
    }

    @Override
    public String getName() {
        return "cluster_get_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean renderDefaults = request.paramAsBoolean("include_defaults", false);

        ClusterGetSettingsAction.Request clusterSettingsRequest = new ClusterGetSettingsAction.Request(getMasterNodeTimeout(request));
        RestUtils.consumeDeprecatedLocalParameter(request);

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            ClusterGetSettingsAction.INSTANCE,
            clusterSettingsRequest,
            new RestToXContentListener<RestClusterGetSettingsResponse>(channel).map(
                r -> response(r, renderDefaults, settingsFilter, clusterSettings, settings)
            )
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    static RestClusterGetSettingsResponse response(
        final ClusterGetSettingsAction.Response response,
        final boolean renderDefaults,
        final SettingsFilter settingsFilter,
        final ClusterSettings clusterSettings,
        final Settings settings
    ) {
        return new RestClusterGetSettingsResponse(
            settingsFilter.filter(response.persistentSettings()),
            settingsFilter.filter(response.transientSettings()),
            renderDefaults ? settingsFilter.filter(clusterSettings.diff(response.settings(), settings)) : Settings.EMPTY
        );
    }

}
