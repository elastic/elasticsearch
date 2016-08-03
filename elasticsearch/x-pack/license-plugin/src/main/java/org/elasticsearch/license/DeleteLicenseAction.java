/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class DeleteLicenseAction extends Action<DeleteLicenseRequest, DeleteLicenseResponse, DeleteLicenseRequestBuilder> {

    public static final DeleteLicenseAction INSTANCE = new DeleteLicenseAction();
    public static final String NAME = "cluster:admin/xpack/license/delete";

    private DeleteLicenseAction() {
        super(NAME);
    }

    @Override
    public DeleteLicenseResponse newResponse() {
        return new DeleteLicenseResponse();
    }

    @Override
    public DeleteLicenseRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new DeleteLicenseRequestBuilder(client, this);
    }
}
