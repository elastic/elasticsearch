/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;

public interface SelfGeneratedLicenseService<
    TrialRequest extends ActionRequest,
    TrialResponse extends ActionResponse,
    BasicRequest extends ActionRequest,
    BasicResponse extends ActionResponse> {

    void startTrialLicense(TrialRequest request, ActionListener<TrialResponse> listener);

    void startBasicLicense(BasicRequest request, ActionListener<BasicResponse> listener);

}
