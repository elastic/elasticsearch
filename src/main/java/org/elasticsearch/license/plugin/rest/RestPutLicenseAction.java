/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends BaseRestHandler {

    @Inject
    public RestPutLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(PUT, "/_licenses", this);
        controller.registerHandler(POST, "/_licenses", this);
    }


    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.listenerThreaded(false);
        putLicenseRequest.licenses(request.content().toUtf8());
        client.admin().cluster().execute(PutLicenseAction.INSTANCE, putLicenseRequest, new LicensesAcknowledgedListener(channel));
    }

    private class LicensesAcknowledgedListener extends AcknowledgedRestListener<PutLicenseResponse> {


        public LicensesAcknowledgedListener(RestChannel channel) {
            super(channel);
        }

        @Override
        protected void addCustomFields(XContentBuilder builder, PutLicenseResponse response) throws IOException {
            LicensesStatus status = response.status();
            String statusString = null;
            switch (status) {
                case VALID:
                    statusString = "valid";
                    break;
                case INVALID:
                    statusString = "invalid";
                    break;
                case EXPIRED:
                    statusString = "expired";
                    break;
            }
            builder.field("licenses_status", statusString);
        }
    }
}
