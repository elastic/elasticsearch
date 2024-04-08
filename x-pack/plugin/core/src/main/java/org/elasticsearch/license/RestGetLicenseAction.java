/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.RestApiVersion.V_7;
import static org.elasticsearch.core.RestApiVersion.V_8;
import static org.elasticsearch.core.RestApiVersion.onOrAfter;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

@ServerlessScope(Scope.PUBLIC)
public class RestGetLicenseAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetLicenseAction.class);

    public RestGetLicenseAction() {}

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(GET, "/_license").replaces(GET, "/_xpack/license", RestApiVersion.V_7).build());
    }

    @Override
    public String getName() {
        return "get_license";
    }

    /**
     * There will be only one license displayed per feature, the selected license will have the latest expiry_date
     * out of all other licenses for the feature.
     * <p>
     * The licenses are sorted by latest issue_date
     */
    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final Map<String, String> overrideParams = Maps.newMapWithExpectedSize(2);
        overrideParams.put(License.REST_VIEW_MODE, "true");
        overrideParams.put(License.LICENSE_VERSION_MODE, String.valueOf(License.VERSION_CURRENT));

        if (request.getRestApiVersion() == V_7) {
            // Hide enterprise licenses by default, there is an opt-in flag to show them
            final boolean hideEnterprise = request.paramAsBoolean("accept_enterprise", false) == false;
            final int licenseVersion = hideEnterprise ? License.VERSION_CRYPTO_ALGORITHMS : License.VERSION_CURRENT;
            overrideParams.put(License.LICENSE_VERSION_MODE, String.valueOf(licenseVersion));
            overrideParams.put(License.XCONTENT_HIDE_ENTERPRISE, String.valueOf(hideEnterprise));
        }
        // In 7.x, there was an opt-in flag to show "enterprise" licenses. In 8.0 the flag is deprecated and can only be true
        // TODO Remove this from 9.0
        if (request.hasParam("accept_enterprise")) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "get_license_accept_enterprise",
                "Including [accept_enterprise] in get license requests is deprecated."
                    + " The parameter will be removed in the next major version"
            );
            if (request.paramAsBoolean("accept_enterprise", true) == false && request.getRestApiVersion().matches(onOrAfter(V_8))) {
                throw new IllegalArgumentException("The [accept_enterprise] parameters may not be false");
            }
        }

        final ToXContent.Params params = new ToXContent.DelegatingMapParams(overrideParams, request);
        GetLicenseRequest getLicenseRequest = new GetLicenseRequest();
        getLicenseRequest.local(request.paramAsBoolean("local", getLicenseRequest.local()));
        return channel -> client.admin()
            .cluster()
            .execute(GetLicenseAction.INSTANCE, getLicenseRequest, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetLicenseResponse response, XContentBuilder builder) throws Exception {
                    // Default to pretty printing, but allow ?pretty=false to disable
                    if (request.hasParam("pretty") == false) {
                        builder.prettyPrint().lfAtEnd();
                    }
                    boolean hasLicense = response.license() != null;
                    builder.startObject();
                    if (hasLicense) {
                        builder.startObject("license");
                        response.license().toInnerXContent(builder, params);
                        builder.endObject();
                    }
                    builder.endObject();
                    return new RestResponse(hasLicense ? OK : NOT_FOUND, builder);
                }
            });
    }

}
