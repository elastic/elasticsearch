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

public interface TrialLicenseService<Request extends ActionRequest, Response extends ActionResponse> {

    void startTrialLicense(Request request, ActionListener<Response> listener);

    // TODO add read trial license
}
