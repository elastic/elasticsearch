/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends BaseRestHandler {

    @Inject
    public RestPutLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(PUT, "/licenses", this);
        controller.registerHandler(POST, "/licenses", this);
    }


    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.listenerThreaded(false);
        putLicenseRequest.license(request.content().toUtf8());
        client.admin().cluster().execute(PutLicenseAction.INSTANCE, putLicenseRequest, new AcknowledgedRestListener<PutLicenseResponse>(channel));
    }
}
