/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestClusterGetSettingsAction extends BaseRestHandler {

    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestClusterGetSettingsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.nodesInCluster = nodesInCluster;
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

        if (nodesInCluster.get().getMinNodeVersion().before(Version.V_8_3_0)) {
            return prepareLegacyRequest(request, client, renderDefaults);
        }

        ClusterGetSettingsAction.Request clusterSettingsRequest = new ClusterGetSettingsAction.Request();

        clusterSettingsRequest.local(request.paramAsBoolean("local", clusterSettingsRequest.local()));
        clusterSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterSettingsRequest.masterNodeTimeout()));

        return channel -> client.admin()
            .cluster()
            .clusterSettings(
                clusterSettingsRequest,
                new RestToXContentListener<RestClusterGetSettingsResponse>(channel).map(
                    response -> response(
                        response.persistentSettings(),
                        response.transientSettings(),
                        response.settings(),
                        renderDefaults,
                        settingsFilter,
                        clusterSettings,
                        settings
                    )
                )
            );
    }

    private RestChannelConsumer prepareLegacyRequest(final RestRequest request, final NodeClient client, final boolean renderDefaults) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().routingTable(false).nodes(false);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        return channel -> client.admin()
            .cluster()
            .state(clusterStateRequest, new RestToXContentListener<RestClusterGetSettingsResponse>(channel).map(response -> {
                ClusterState state = response.getState();
                return response(
                    state.metadata().persistentSettings(),
                    state.metadata().transientSettings(),
                    state.metadata().settings(),
                    renderDefaults,
                    settingsFilter,
                    clusterSettings,
                    settings
                );
            }));
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
        final Settings persistentSettings,
        final Settings transientSettings,
        final Settings allSettings,
        final boolean renderDefaults,
        final SettingsFilter settingsFilter,
        final ClusterSettings clusterSettings,
        final Settings settings
    ) {
        return new RestClusterGetSettingsResponse(
            settingsFilter.filter(persistentSettings),
            settingsFilter.filter(transientSettings),
            renderDefaults ? settingsFilter.filter(clusterSettings.diff(allSettings, settings)) : Settings.EMPTY
        );
    }

}
