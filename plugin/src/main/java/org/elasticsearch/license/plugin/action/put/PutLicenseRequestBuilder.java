/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.license.core.License;

import java.util.List;

/**
 * Register license request builder
 */
public class PutLicenseRequestBuilder extends AcknowledgedRequestBuilder<PutLicenseRequest, PutLicenseResponse, PutLicenseRequestBuilder, ClusterAdminClient> {

    /**
     * Constructs register license request
     *
     * @param clusterAdminClient cluster admin client
     */
    public PutLicenseRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new PutLicenseRequest());
    }

    /**
     * Sets the license
     *
     * @param licenses license
     * @return this builder
     */
    public PutLicenseRequestBuilder setLicense(List<License> licenses) {
        request.licenses(licenses);
        return this;
    }

    public PutLicenseRequestBuilder setLicense(String licenseSource) {
        request.licenses(licenseSource);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutLicenseResponse> listener) {
        client.execute(PutLicenseAction.INSTANCE, request, listener);
    }
}
