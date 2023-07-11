/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlCompleteLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlCompleteLogoutRequest;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.saml.SamlLogoutResponseHandler;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;

import java.util.List;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for completing SAML LogoutResponse
 */
public final class TransportSamlCompleteLogoutAction extends HandledTransportAction<SamlCompleteLogoutRequest, ActionResponse.Empty> {

    private final Realms realms;

    @Inject
    public TransportSamlCompleteLogoutAction(TransportService transportService, ActionFilters actionFilters, Realms realms) {
        super(SamlCompleteLogoutAction.NAME, transportService, actionFilters, SamlCompleteLogoutRequest::new);
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, SamlCompleteLogoutRequest request, ActionListener<ActionResponse.Empty> listener) {
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealm(), null);
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm with name [{}]", request.getRealm()));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] with name [{}]", realms, request.getRealm()));
        } else {
            processLogoutResponse(realms.get(0), request, listener);
        }
    }

    private static void processLogoutResponse(
        SamlRealm samlRealm,
        SamlCompleteLogoutRequest request,
        ActionListener<ActionResponse.Empty> listener
    ) {

        final SamlLogoutResponseHandler logoutResponseHandler = samlRealm.getLogoutResponseHandler();
        try {
            logoutResponseHandler.handle(request.isHttpRedirect(), request.getPayload(), request.getValidRequestIds());
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
