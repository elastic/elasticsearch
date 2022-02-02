/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

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

    public void deleteLicense(DeleteLicenseRequest request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteLicenseAction.INSTANCE, request, listener);
    }

    public PostStartTrialRequestBuilder preparePostStartTrial() {
        return new PostStartTrialRequestBuilder(client);
    }

    public GetTrialStatusRequestBuilder prepareGetStartTrial() {
        return new GetTrialStatusRequestBuilder(client);
    }

    public void postStartTrial(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener) {
        client.execute(PostStartTrialAction.INSTANCE, request, listener);
    }

    public void postStartBasic(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener) {
        client.execute(PostStartBasicAction.INSTANCE, request, listener);
    }

    public PostStartBasicRequestBuilder preparePostStartBasic() {
        return new PostStartBasicRequestBuilder(client);
    }

    public GetBasicStatusRequestBuilder prepareGetStartBasic() {
        return new GetBasicStatusRequestBuilder(client);
    }
}
