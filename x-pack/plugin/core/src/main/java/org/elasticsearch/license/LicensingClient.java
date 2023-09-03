/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.client.internal.ElasticsearchClient;

public class LicensingClient {

    private final ElasticsearchClient client;

    public LicensingClient(ElasticsearchClient client) {
        this.client = client;
    }

    public PutLicenseRequestBuilder preparePutLicense(License license) {
        return new PutLicenseRequestBuilder(client).setLicense(license);
    }

    public GetLicenseRequestBuilder prepareGetLicense() {
        return new GetLicenseRequestBuilder(client);
    }

    public DeleteLicenseRequestBuilder prepareDeleteLicense() {
        return new DeleteLicenseRequestBuilder(client);
    }

    public PostStartTrialRequestBuilder preparePostStartTrial() {
        return new PostStartTrialRequestBuilder(client);
    }

    public GetTrialStatusRequestBuilder prepareGetStartTrial() {
        return new GetTrialStatusRequestBuilder(client);
    }

    public PostStartBasicRequestBuilder preparePostStartBasic() {
        return new PostStartBasicRequestBuilder(client);
    }

    public GetBasicStatusRequestBuilder prepareGetStartBasic() {
        return new GetBasicStatusRequestBuilder(client);
    }
}
