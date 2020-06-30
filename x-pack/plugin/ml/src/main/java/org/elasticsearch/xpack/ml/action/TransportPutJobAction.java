/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.job.JobManager;

import java.io.IOException;

public class TransportPutJobAction extends TransportMasterNodeAction<PutJobAction.Request, PutJobAction.Response> {

    private final JobManager jobManager;
    private final XPackLicenseState licenseState;
    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportPutJobAction(TransportService transportService, ClusterService clusterService,
                                 ThreadPool threadPool, XPackLicenseState licenseState, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver, JobManager jobManager,
                                 AnalysisRegistry analysisRegistry) {
        super(PutJobAction.NAME, transportService, clusterService, threadPool, actionFilters, PutJobAction.Request::new,
            indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.jobManager = jobManager;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutJobAction.Response read(StreamInput in) throws IOException {
        return new PutJobAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, PutJobAction.Request request, ClusterState state,
                                   ActionListener<PutJobAction.Response> listener) throws Exception {
        jobManager.putJob(request, analysisRegistry, state, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, PutJobAction.Request request, ActionListener<PutJobAction.Response> listener) {
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
