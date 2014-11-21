/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.delete;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class DeleteLicenseAction extends ClusterAction<DeleteLicenseRequest, DeleteLicenseResponse, DeleteLicenseRequestBuilder> {

    public static final DeleteLicenseAction INSTANCE = new DeleteLicenseAction();
    public static final String NAME = "cluster:admin/plugin/license/delete";

    private DeleteLicenseAction() {
        super(NAME);
    }

    @Override
    public DeleteLicenseResponse newResponse() {
        return new DeleteLicenseResponse();
    }

    @Override
    public DeleteLicenseRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new DeleteLicenseRequestBuilder(client);
    }
}