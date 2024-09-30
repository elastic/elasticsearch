/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteLicenseAction extends AcknowledgedTransportMasterNodeAction<AcknowledgedRequest.Plain> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/xpack/license/delete");
    private final MutableLicenseService licenseService;

    @Inject
    public TransportDeleteLicenseAction(
        TransportService transportService,
        ClusterService clusterService,
        MutableLicenseService licenseService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AcknowledgedRequest.Plain::new,
            indexNameExpressionResolver,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.licenseService = licenseService;
    }

    @Override
    protected ClusterBlockException checkBlock(AcknowledgedRequest.Plain request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final AcknowledgedRequest.Plain request,
        ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) throws ElasticsearchException {
        licenseService.removeLicense(
            request.masterNodeTimeout(),
            request.ackTimeout(),
            listener.map(r -> AcknowledgedResponse.of(r.isAcknowledged()))
        );
    }
}
