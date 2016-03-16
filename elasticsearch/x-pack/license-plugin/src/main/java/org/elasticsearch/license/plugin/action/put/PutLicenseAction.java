/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class PutLicenseAction extends Action<PutLicenseRequest, PutLicenseResponse, PutLicenseRequestBuilder> {

    public static final PutLicenseAction INSTANCE = new PutLicenseAction();
    public static final String NAME = "cluster:admin/xpack/license/put";

    private PutLicenseAction() {
        super(NAME);
    }

    @Override
    public PutLicenseResponse newResponse() {
        return new PutLicenseResponse();
    }

    @Override
    public PutLicenseRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PutLicenseRequestBuilder(client, this);
    }
}
