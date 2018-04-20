/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;
import org.elasticsearch.xpack.core.XPackField;

import java.util.function.Supplier;

public abstract class WatcherTransportAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse>
        extends TransportMasterNodeAction<Request, Response> {

    protected final XPackLicenseState licenseState;
    private final ClusterService clusterService;
    private final Supplier<Response> response;

    public WatcherTransportAction(Settings settings, String actionName, TransportService transportService, ThreadPool threadPool,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                  XPackLicenseState licenseState, ClusterService clusterService, Supplier<Request> request,
                                  Supplier<Response> response) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, request);
        this.licenseState = licenseState;
        this.clusterService = clusterService;
        this.response = response;
    }

    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected Response newResponse() {
        return response.get();
    }

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception;

    protected boolean localExecute(Request request) {
        return WatcherLifeCycleService.isWatchExecutionDistributed(clusterService.state());
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        IndexMetaData index = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metaData());
        if (index != null) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, index.getIndex().getName());
        } else {
            return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
        }
    }

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (licenseState.isWatcherAllowed()) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.WATCHER));
        }
    }
}
