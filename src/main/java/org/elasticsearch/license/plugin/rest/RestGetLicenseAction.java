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
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequest;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetLicenseAction extends BaseRestHandler {

    @Inject
    public RestGetLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_licenses", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        client.admin().cluster().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(), new RestBuilderListener<GetLicenseResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetLicenseResponse response, XContentBuilder builder) throws Exception {
                toXContent(response, builder);
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    /**
     * Output Format:
     *   {
     *     "licenses" : [
     *        {
     *         "uid" : ...,
     *          "type" : ...,
     *          "subscription_type" :...,
     *          "issued_to" : ... (cluster name if one-time trial license, else value from signed license),
     *          "issue_date" : YY-MM-DD (date string in UTC),
     *          "expiry_date" : YY-MM-DD (date string in UTC),
     *          "feature" : ...,
     *          "max_nodes" : ...
     *        },
     *        {...}
     *      ]
     *   }
     *
     * There will be only one license displayed per feature, the selected license will have the latest expiry_date
     * out of all other licenses for the feature.
     *
     * The licenses are sorted by latest issue_date
     */
    private static void toXContent(GetLicenseResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("licenses");
        for (ESLicense license : response.licenses()) {
            builder.startObject();

            builder.field("uid", license.uid());
            builder.field("type", license.type());
            builder.field("subscription_type", license.subscriptionType());
            builder.field("issued_to", license.issuedTo());
            builder.field("issue_date", DateUtils.dateStringFromLongDate(license.issueDate()));
            builder.field("expiry_date", DateUtils.dateStringFromLongDate(license.expiryDate()));
            builder.field("feature", license.feature());
            builder.field("max_nodes", license.maxNodes());

            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }
}
