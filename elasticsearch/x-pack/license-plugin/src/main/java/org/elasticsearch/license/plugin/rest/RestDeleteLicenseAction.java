/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteLicenseAction extends BaseRestHandler {

    @Inject
    public RestDeleteLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(DELETE, "/_license", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        client.admin().cluster().execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest(), new AcknowledgedRestListener<>(channel));
    }
}
