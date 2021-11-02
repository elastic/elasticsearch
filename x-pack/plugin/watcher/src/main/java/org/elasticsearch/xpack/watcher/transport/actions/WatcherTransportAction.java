/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.watcher.WatcherField;

abstract class WatcherTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends HandledTransportAction<
    Request,
    Response> {

    protected final XPackLicenseState licenseState;

    WatcherTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        Writeable.Reader<Request> request
    ) {
        super(actionName, transportService, actionFilters, request);
        this.licenseState = licenseState;
    }

    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected final void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (WatcherField.WATCHER_FEATURE.check(licenseState)) {
            doExecute(request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.WATCHER));
        }
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);
}
