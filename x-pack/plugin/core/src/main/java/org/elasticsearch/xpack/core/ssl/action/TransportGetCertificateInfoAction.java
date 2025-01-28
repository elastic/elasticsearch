/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

public class TransportGetCertificateInfoAction extends HandledTransportAction<
    GetCertificateInfoAction.Request,
    GetCertificateInfoAction.Response> {

    private final SSLService sslService;

    @Inject
    public TransportGetCertificateInfoAction(TransportService transportService, ActionFilters actionFilters, SSLService sslService) {
        super(
            GetCertificateInfoAction.NAME,
            transportService,
            actionFilters,
            GetCertificateInfoAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.sslService = sslService;
    }

    @Override
    protected void doExecute(
        Task task,
        GetCertificateInfoAction.Request request,
        ActionListener<GetCertificateInfoAction.Response> listener
    ) {
        try {
            Collection<CertificateInfo> certificates = sslService.getLoadedCertificates();
            listener.onResponse(new GetCertificateInfoAction.Response(certificates));
        } catch (GeneralSecurityException | IOException e) {
            listener.onFailure(e);
        }
    }
}
