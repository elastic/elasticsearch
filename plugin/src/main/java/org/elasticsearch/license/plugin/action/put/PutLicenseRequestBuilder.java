/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.license.core.License;

import java.util.List;

/**
 * Register license request builder
 */
public class PutLicenseRequestBuilder extends AcknowledgedRequestBuilder<PutLicenseRequest, PutLicenseResponse, PutLicenseRequestBuilder> {

    /**
     * Constructs register license request
     *
     * @param client elasticsearch client
     */
    public PutLicenseRequestBuilder(ElasticsearchClient client, PutLicenseAction action) {
        super(client, action, new PutLicenseRequest());
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
}
