/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetTrialStatus extends XPackRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetTrialStatus.class));

    RestGetTrialStatus(Settings settings, RestController controller) {
        super(settings);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, "/_license/trial_status", this,
                GET, URI_BASE + "/license/trial_status", deprecationLogger);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) {
        return channel -> client.licensing().prepareGetStartTrial().execute(new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_trial_status";
    }

}
