/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequest;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.get.TransportGetLicenseAction;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.client.Requests.getRepositoryRequest;
import static org.elasticsearch.license.plugin.action.Utils.licenseAsMap;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetLicenseAction extends BaseRestHandler {

    private final TransportGetLicenseAction transportGetLicenseAction;

    @Inject
    public RestGetLicenseAction(Settings settings, RestController controller, Client client, TransportGetLicenseAction transportGetLicenseAction) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cluster/license", this);
        this.transportGetLicenseAction = transportGetLicenseAction;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        transportGetLicenseAction.execute(new GetLicenseRequest(), new RestBuilderListener<GetLicenseResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetLicenseResponse getLicenseResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.startArray("licenses");
                for (ESLicenses.ESLicense license : getLicenseResponse.licenses()) {
                    builder.map(licenseAsMap(license));
                }
                builder.endArray();
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
        //client.admin().cluster().execute(GetLicenseAction.INSTANCE, getLicenseRequest, )
    }
}
