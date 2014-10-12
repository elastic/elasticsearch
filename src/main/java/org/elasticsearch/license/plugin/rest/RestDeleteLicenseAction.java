/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteLicenseAction extends BaseRestHandler {

    @Inject
    public RestDeleteLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, "/_cluster/license/{features}", this);
    }


    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] features = Strings.splitStringByCommaToArray(request.param("features"));
        if (features.length == 0) {
            throw new ElasticsearchIllegalArgumentException("no features specified for license deletion");
        }
        DeleteLicenseRequest deleteLicenseRequest = new DeleteLicenseRequest(getFeaturesToDelete(features));
        deleteLicenseRequest.listenerThreaded(false);
        client.admin().cluster().execute(DeleteLicenseAction.INSTANCE, deleteLicenseRequest, new AcknowledgedRestListener<DeleteLicenseResponse>(channel));
    }

    private static String[] getFeaturesToDelete(String[] features) {
        Set<String> result = new HashSet<>();
        for (String feature : features) {
            if (feature.equalsIgnoreCase("_all")) {
                for (FeatureType featureType : FeatureType.values()) {
                    result.add(featureType.string());
                }
                break;
            } else {
                result.add(FeatureType.fromString(feature).string());
            }
        }
        return result.toArray(new String[result.size()]);
    }
}
