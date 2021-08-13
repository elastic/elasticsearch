/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.license.StartTrialRequest;
import org.elasticsearch.client.license.StartBasicRequest;
import org.elasticsearch.client.license.DeleteLicenseRequest;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.PutLicenseRequest;

final class LicenseRequestConverters {

    private LicenseRequestConverters() {}

    static Request putLicense(PutLicenseRequest putLicenseRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_license").build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(putLicenseRequest.timeout());
        parameters.withMasterTimeout(putLicenseRequest.masterNodeTimeout());
        if (putLicenseRequest.isAcknowledge()) {
            parameters.putParam("acknowledge", "true");
        }
        request.addParameters(parameters.asMap());
        request.setJsonEntity(putLicenseRequest.getLicenseDefinition());
        return request;
    }

    static Request getLicense(GetLicenseRequest getLicenseRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_license").build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withLocal(getLicenseRequest.isLocal());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request deleteLicense(DeleteLicenseRequest deleteLicenseRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_license").build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(deleteLicenseRequest.timeout());
        parameters.withMasterTimeout(deleteLicenseRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request startTrial(StartTrialRequest startTrialRequest) {
        final String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_license", "start_trial").build();
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.putParam("acknowledge", Boolean.toString(startTrialRequest.isAcknowledge()));
        if (startTrialRequest.getLicenseType() != null) {
            parameters.putParam("type", startTrialRequest.getLicenseType());
        }
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request startBasic(StartBasicRequest startBasicRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_license", "start_basic")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(startBasicRequest.timeout());
        parameters.withMasterTimeout(startBasicRequest.masterNodeTimeout());
        if (startBasicRequest.isAcknowledge()) {
            parameters.putParam("acknowledge", "true");
        }
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request getLicenseTrialStatus() {
        return new Request(HttpGet.METHOD_NAME, "/_license/trial_status");
    }

    static Request getLicenseBasicStatus() {
        return new Request(HttpGet.METHOD_NAME, "/_license/basic_status");
    }

}
