/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteLicenseAction extends XPackRestHandler {

    @Inject
    public RestDeleteLicenseAction(Settings settings, RestController controller) {
        super(settings);
        // @deprecated Remove deprecations in 6.0
        controller.registerWithDeprecatedHandler(DELETE, URI_BASE + "/license", this,
                                                 DELETE, "/_license", deprecationLogger);

        // Remove _licenses support entirely in 6.0
        controller.registerAsDeprecatedHandler(DELETE, "/_licenses", this,
                                               "[DELETE /_licenses] is deprecated! Use " +
                                               "[DELETE /_xpack/license] instead.",
                                               deprecationLogger);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final XPackClient client) {
        client.es().admin().cluster().execute(DeleteLicenseAction.INSTANCE,
                                              new DeleteLicenseRequest(),
                                              new AcknowledgedRestListener<>(channel));
    }
}
