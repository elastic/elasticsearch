/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class PutLicenseAction extends ClusterAction<PutLicenseRequest, PutLicenseResponse, PutLicenseRequestBuilder> {

    public static final PutLicenseAction INSTANCE = new PutLicenseAction();
    public static final String NAME = "cluster:admin/plugin/license/put";

    private PutLicenseAction() {
        super(NAME);
    }

    @Override
    public PutLicenseResponse newResponse() {
        return new PutLicenseResponse();
    }

    @Override
    public PutLicenseRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new PutLicenseRequestBuilder(client);
    }
}
