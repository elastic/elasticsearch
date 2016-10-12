/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ElasticsearchClient;

public class LicensingClient {

    private final ElasticsearchClient client;

    public LicensingClient(ElasticsearchClient client) {
        this.client = client;
    }

    public PutLicenseRequestBuilder preparePutLicense(License license) {
        return new PutLicenseRequestBuilder(client).setLicense(license);
    }

    public void putLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener) {
        client.execute(PutLicenseAction.INSTANCE, request, listener);
    }

    public GetLicenseRequestBuilder prepareGetLicense() {
        return new GetLicenseRequestBuilder(client);
    }

    public void getLicense(GetLicenseRequest request, ActionListener<GetLicenseResponse> listener) {
        client.execute(GetLicenseAction.INSTANCE, request, listener);
    }

    public DeleteLicenseRequestBuilder prepareDeleteLicense() {
        return new DeleteLicenseRequestBuilder(client);
    }

    public void deleteLicense(DeleteLicenseRequest request, ActionListener<DeleteLicenseResponse> listener) {
        client.execute(DeleteLicenseAction.INSTANCE, request, listener);
    }
}
