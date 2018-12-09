/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;

public class GetLicenseAction extends Action<GetLicenseRequest, GetLicenseResponse, GetLicenseRequestBuilder> {

    public static final GetLicenseAction INSTANCE = new GetLicenseAction();
    public static final String NAME = "cluster:monitor/xpack/license/get";

    private GetLicenseAction() {
        super(NAME);
    }

    @Override
    public GetLicenseResponse newResponse() {
        return new GetLicenseResponse();
    }

    @Override
    public GetLicenseRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetLicenseRequestBuilder(client, this);
    }
}
