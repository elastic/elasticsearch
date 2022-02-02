/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClusterRerouteAction extends BaseRestHandler {
    private static final ObjectParser<ClusterRerouteRequest, Void> PARSER = new ObjectParser<>("cluster_reroute");
    static {
        PARSER.declareField(
            (p, v, c) -> v.commands(AllocationCommands.fromXContent(p)),
            new ParseField("commands"),
            ValueType.OBJECT_ARRAY
        );
        PARSER.declareBoolean(ClusterRerouteRequest::dryRun, new ParseField("dry_run"));
    }

    private static final String DEFAULT_METRICS = Strings.arrayToCommaDelimitedString(
        EnumSet.complementOf(EnumSet.of(ClusterState.Metric.METADATA)).toArray()
    );

    private final SettingsFilter settingsFilter;

    public RestClusterRerouteAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_cluster/reroute"));
    }

    @Override
    public String getName() {
        return "cluster_reroute_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = createRequest(request);
        settingsFilter.addFilterSettingParams(request);
        if (clusterRerouteRequest.explain()) {
            request.params().put("explain", Boolean.TRUE.toString());
        }
        // by default, return everything but metadata
        final String metric = request.param("metric");
        if (metric == null) {
            request.params().put("metric", DEFAULT_METRICS);
        }
        return channel -> client.admin().cluster().reroute(clusterRerouteRequest, new RestToXContentListener<>(channel));
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>();
        responseParams.add("metric");
        responseParams.addAll(Settings.FORMAT_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    public static ClusterRerouteRequest createRequest(RestRequest request) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = Requests.clusterRerouteRequest();
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.explain(request.paramAsBoolean("explain", clusterRerouteRequest.explain()));
        clusterRerouteRequest.timeout(request.paramAsTime("timeout", clusterRerouteRequest.timeout()));
        clusterRerouteRequest.setRetryFailed(request.paramAsBoolean("retry_failed", clusterRerouteRequest.isRetryFailed()));
        clusterRerouteRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterRerouteRequest.masterNodeTimeout()));
        request.applyContentParser(parser -> PARSER.parse(parser, clusterRerouteRequest, null));
        return clusterRerouteRequest;
    }
}
