/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;

/**
 * Base class for security rest handlers. This handler takes care of ensuring that the license
 * level is valid so that security can be used!
 */
public abstract class SecurityBaseRestHandler extends BaseRestHandler {

    protected final Settings settings;
    protected final XPackLicenseState licenseState;

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    protected SecurityBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        this.settings = settings;
        this.licenseState = licenseState;
    }

    /**
     * Calls the {@link #innerPrepareRequest(RestRequest, NodeClient)} method and then checks the
     * license state. If the license state allows auth, the result from
     * {@link #innerPrepareRequest(RestRequest, NodeClient)} is returned, otherwise a default error
     * response will be returned indicating that security is not licensed.
     *
     * Note: the implementing rest handler is called before the license is checked so that we do not
     * trip the unused parameters check
     */
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RestChannelConsumer consumer = innerPrepareRequest(request, client);
        final Exception failedFeature = checkFeatureAvailable(request);
        if (failedFeature == null) {
            return consumer;
        } else {
            return channel -> channel.sendResponse(new RestResponse(channel, failedFeature));
        }
    }

    /**
     * Check whether the given request is allowed within the current license state and setup,
     * and return the name of any unlicensed feature.
     * By default this returns an exception if security is not enabled.
     * Sub-classes can override this method if they have additional requirements.
     *
     * @return {@code null} if all required features are available, otherwise an exception to be
     * sent to the requestor
     */
    protected Exception checkFeatureAvailable(RestRequest request) {
        if (XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return new IllegalStateException("Security is not enabled but a security rest handler is registered");
        } else {
            return null;
        }
    }

    /**
     * Implementers should implement this method as they normally would for
     * {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)} and ensure that all request
     * parameters are consumed prior to returning a value. The returned value is not guaranteed to
     * be executed unless security is licensed and all request parameters are known
     */
    protected abstract RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException;
}
