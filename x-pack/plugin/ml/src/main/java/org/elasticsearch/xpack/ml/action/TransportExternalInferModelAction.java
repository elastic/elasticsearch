/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

public class TransportExternalInferModelAction extends TransportInternalInferModelAction {
    @Inject
    public TransportExternalInferModelAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TrainedModelProvider trainedModelProvider
    ) {
        super(
            InferModelAction.EXTERNAL_NAME,
            transportService,
            actionFilters,
            modelLoadingService,
            client,
            clusterService,
            licenseState,
            trainedModelProvider
        );
    }
}
