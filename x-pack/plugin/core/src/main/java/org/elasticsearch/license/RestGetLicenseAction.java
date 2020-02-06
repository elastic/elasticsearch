/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetLicenseAction extends XPackRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetLicenseAction.class));

    RestGetLicenseAction() {}

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(GET, "/_license", GET,  URI_BASE + "/license", deprecationLogger));
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
    public RestChannelConsumer doPrepareRequest(final RestRequest request, final XPackClient client) throws IOException {
        // Hide enterprise licenses by default, there is an opt-in flag to show them
        final boolean hideEnterprise = request.paramAsBoolean("accept_enterprise", false) == false;
        final int licenseVersion = hideEnterprise ? License.VERSION_CRYPTO_ALGORITHMS : License.VERSION_CURRENT;

        final Map<String, String> overrideParams = new HashMap<>(3);
        overrideParams.put(License.REST_VIEW_MODE, "true");
        overrideParams.put(License.LICENSE_VERSION_MODE, String.valueOf(licenseVersion));
        overrideParams.put(License.XCONTENT_HIDE_ENTERPRISE, String.valueOf(hideEnterprise));

        final ToXContent.Params params = new ToXContent.DelegatingMapParams(overrideParams, request);
        GetLicenseRequest getLicenseRequest = new GetLicenseRequest();
        getLicenseRequest.local(request.paramAsBoolean("local", getLicenseRequest.local()));
        return channel -> client.es().admin().cluster().execute(GetLicenseAction.INSTANCE, getLicenseRequest,
                new RestBuilderListener<GetLicenseResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(GetLicenseResponse response, XContentBuilder builder) throws Exception {
                        // Default to pretty printing, but allow ?pretty=false to disable
                        if (!request.hasParam("pretty")) {
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
                        return new BytesRestResponse(hasLicense ? OK : NOT_FOUND, builder);
                    }
                });
    }

}
