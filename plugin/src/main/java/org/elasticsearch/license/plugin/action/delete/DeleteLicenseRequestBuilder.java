/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

import java.util.Set;

public class DeleteLicenseRequestBuilder extends AcknowledgedRequestBuilder<DeleteLicenseRequest, DeleteLicenseResponse, DeleteLicenseRequestBuilder, ClusterAdminClient> {

    /**
     * Creates new get licenses request builder
     *
     * @param clusterAdminClient cluster admin client
     */
    public DeleteLicenseRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new DeleteLicenseRequest());
    }

    public DeleteLicenseRequestBuilder setFeatures(Set<String> features) {
        request.features(features);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<DeleteLicenseResponse> listener) {
        client.execute(DeleteLicenseAction.INSTANCE, request, listener);
    }
}