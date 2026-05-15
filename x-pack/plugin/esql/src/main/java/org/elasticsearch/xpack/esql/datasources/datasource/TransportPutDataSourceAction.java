/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.crypto.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;

public class TransportPutDataSourceAction extends AcknowledgedTransportMasterNodeProjectAction<PutDataSourceAction.Request> {
    private final DataSourceService dataSourceService;
    private final FeatureService featureService;

    /**
     * Wired by an optional Guice setter ({@link #setEncryptionService}) — the security plugin binds
     * {@link EncryptionService} only when the primary-encryption-key feature flag is on. When off
     * (or when the security plugin is absent in test environments), the setter is never called and
     * this field stays null; the put path then skips encryption — equivalent to the mixed-version
     * fallback documented on {@link DataSourceService#putDataSource}.
     *
     * <p>This Guice fork supports {@code @Inject(optional = true)} only on methods and constructors,
     * not fields — hence the setter rather than a direct field annotation.
     */
    private volatile EncryptionService encryptionService;

    @Inject(optional = true)
    public void setEncryptionService(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    @Inject
    public TransportPutDataSourceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataSourceService dataSourceService,
        FeatureService featureService,
        ProjectResolver projectResolver
    ) {
        super(
            PutDataSourceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDataSourceAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.dataSourceService = dataSourceService;
        this.featureService = featureService;
    }

    @Override
    protected void doExecute(Task task, PutDataSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // Coord-side pre-check: validator dispatch. Fails fast without a master round-trip
        // on unknown type or validation failure. The task body re-validates under CAS.
        try {
            dataSourceService.validatePutDataSource(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDataSourceAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Plant the service into the data-node decrypt seam idempotently — runs on every PUT but is
        // a no-op after the first call, and a no-op forever if EncryptionService is unbound.
        if (encryptionService != null) {
            DataSourceCredentials.initialize(encryptionService);
        }
        dataSourceService.putDataSource(state.projectId(), request, encryptionService, featureService, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataSourceAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
