/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XpackField;

public abstract class WatcherTransportAction<Request extends ActionRequest, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    protected final XPackLicenseState licenseState;

    public WatcherTransportAction(Settings settings, String actionName, TransportService transportService, ThreadPool threadPool,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                  XPackLicenseState licenseState, Writeable.Reader<Request> request) {
        super(settings, actionName, threadPool, transportService, actionFilters, request, indexNameExpressionResolver);
        this.licenseState = licenseState;
    }

    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (licenseState.isWatcherAllowed()) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XpackField.WATCHER));
        }
    }
}
