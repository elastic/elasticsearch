/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterRerouteAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterRerouteAction.class);

    private static final Set<String> RESPONSE_PARAMS = addToCopy(Settings.FORMAT_PARAMS, "metric");

    private static final ObjectParser<ClusterRerouteRequest, Void> PARSER = new ObjectParser<>("cluster_reroute");
    static {
        PARSER.declareField(
            (p, v, c) -> v.commands(AllocationCommands.fromXContent(p)),
            new ParseField("commands"),
            ValueType.OBJECT_ARRAY
        );
        PARSER.declareBoolean(ClusterRerouteRequest::dryRun, new ParseField("dry_run"));
    }

    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // no longer used, so can be removed
    private static final String V8_DEFAULT_METRICS = Strings.arrayToCommaDelimitedString(
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

    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    // actually UpdateForV11 because V10 still supports the V9 API including this deprecation message
    private static final String METRIC_DEPRECATION_MESSAGE = """
        the [?metric] query parameter to the [POST /_cluster/reroute] API has no effect; its use will be forbidden in a future version""";

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = createRequest(request);
        settingsFilter.addFilterSettingParams(request);
        if (clusterRerouteRequest.explain()) {
            request.params().put("explain", Boolean.TRUE.toString());
        }

        if (request.getRestApiVersion().matches(RestApiVersion.onOrAfter(RestApiVersion.V_9))) {
            // always avoid returning the cluster state by forcing `?metric=none`; emit a warning if `?metric` is even present
            if (request.hasParam("metric")) {
                deprecationLogger.critical(DeprecationCategory.API, "cluster-reroute-metric-param", METRIC_DEPRECATION_MESSAGE);
            }
            request.params().put("metric", "none");
        } else {
            assert request.getRestApiVersion().matches(RestApiVersion.equalTo(RestApiVersion.V_8));
            @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // forbid this parameter in the v10 API
            // by default, return everything but metadata
            final String metric = request.param("metric");
            if (metric == null) {
                request.params().put("metric", V8_DEFAULT_METRICS);
            }
        }

        return channel -> client.execute(
            TransportClusterRerouteAction.TYPE,
            clusterRerouteRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );

    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    public static ClusterRerouteRequest createRequest(RestRequest request) throws IOException {
        final var clusterRerouteRequest = new ClusterRerouteRequest(getMasterNodeTimeout(request), getAckTimeout(request));
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.explain(request.paramAsBoolean("explain", clusterRerouteRequest.explain()));
        clusterRerouteRequest.setRetryFailed(request.paramAsBoolean("retry_failed", clusterRerouteRequest.isRetryFailed()));
        request.applyContentParser(parser -> PARSER.parse(parser, clusterRerouteRequest, null));
        return clusterRerouteRequest;
    }
}
