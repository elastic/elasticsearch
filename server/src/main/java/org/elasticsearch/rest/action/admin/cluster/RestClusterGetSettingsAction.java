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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
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

    private void setUpRequestParams(MasterNodeReadRequest<?> clusterRequest, RestRequest request) {
        clusterRequest.local(request.paramAsBoolean("local", clusterRequest.local()));
        clusterRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterRequest.masterNodeTimeout()));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean renderDefaults = request.paramAsBoolean("include_defaults", false);

        if (nodesInCluster.get().getMinNodeVersion().before(Version.V_8_3_0)) {
            return prepareLegacyRequest(request, client, renderDefaults);
        }

        ClusterGetSettingsAction.Request clusterSettingsRequest = new ClusterGetSettingsAction.Request();

        setUpRequestParams(clusterSettingsRequest, request);

        return channel -> client.execute(
            ClusterGetSettingsAction.INSTANCE,
            clusterSettingsRequest,
            new RestToXContentListener<RestClusterGetSettingsResponse>(channel).map(
                r -> response(r, renderDefaults, settingsFilter, clusterSettings, settings)
            )
        );
    }

    private RestChannelConsumer prepareLegacyRequest(final RestRequest request, final NodeClient client, final boolean renderDefaults) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest().routingTable(false).nodes(false);
        setUpRequestParams(clusterStateRequest, request);
        return channel -> client.admin()
            .cluster()
            .state(
                clusterStateRequest,
                new RestToXContentListener<RestClusterGetSettingsResponse>(channel).map(
                    r -> response(
                        new ClusterGetSettingsAction.Response(
                            r.getState().metadata().persistentSettings(),
                            r.getState().metadata().transientSettings(),
                            r.getState().metadata().settings()
                        ),
                        renderDefaults,
                        settingsFilter,
                        clusterSettings,
                        settings
                    )
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
