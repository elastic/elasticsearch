/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestPostAnalyticsEventAction extends EnterpriseSearchBaseRestHandler {
    public RestPostAnalyticsEventAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    public static final String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";

    @Override
    public String getName() {
        return "analytics_post_event_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/" + EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT + "/{collection_name}/event/{event_type}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) {
        PostAnalyticsEventAction.Request request = buidRequest(restRequest);
        return channel -> client.execute(PostAnalyticsEventAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

    private InetAddress getClientAddress(RestRequest restRequest, Map<String, List<String>> headers) {
        InetAddress remoteAddress = restRequest.getHttpChannel().getRemoteAddress().getAddress();
        if (headers.containsKey(X_FORWARDED_FOR_HEADER)) {
            final List<String> addresses = headers.get(X_FORWARDED_FOR_HEADER);
            if (addresses.isEmpty() == false) {
                try {
                    remoteAddress = InetAddresses.forString(addresses.get(0));
                } catch (IllegalArgumentException e) {
                    // Ignore if malformed IP
                }
            }
        }
        return remoteAddress;
    }

    private PostAnalyticsEventAction.Request buidRequest(RestRequest restRequest) {
        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();

        PostAnalyticsEventAction.RequestBuilder builder = PostAnalyticsEventAction.Request.builder(
            restRequest.param("collection_name"),
            restRequest.param("event_type"),
            sourceTuple.v1(),
            sourceTuple.v2()
        );

        builder.debug(restRequest.paramAsBoolean("debug", false));

        final Map<String, List<String>> headers = restRequest.getHeaders();
        builder.headers(headers);
        builder.clientAddress(getClientAddress(restRequest, headers));

        return builder.request();
    }
}
