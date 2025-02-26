/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestDeleteLicenseAction extends BaseRestHandler {

    public RestDeleteLicenseAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_license"));
    }

    @Override
    public String getName() {
        return "delete_license";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var deleteLicenseRequest = new AcknowledgedRequest.Plain(getMasterNodeTimeout(request), getAckTimeout(request));
        return channel -> client.admin()
            .cluster()
            .execute(TransportDeleteLicenseAction.TYPE, deleteLicenseRequest, new RestToXContentListener<>(channel));
    }
}
