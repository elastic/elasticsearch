/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlVerifyLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlVerifyLogoutRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlVerifyLogoutResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.saml.SamlLogoutResponseHandler;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;

import java.util.List;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for taking saml content and turning it into a token.
 */
public final class TransportSamlVerifyLogoutAction extends HandledTransportAction<SamlVerifyLogoutRequest, SamlVerifyLogoutResponse> {

    private final Realms realms;

    @Inject
    public TransportSamlVerifyLogoutAction(TransportService transportService, ActionFilters actionFilters, Realms realms) {
        super(SamlVerifyLogoutAction.NAME, transportService, actionFilters, SamlVerifyLogoutRequest::new);
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, SamlVerifyLogoutRequest request, ActionListener<SamlVerifyLogoutResponse> listener) {
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealm(), request.getAssertionConsumerServiceURL());
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm for [{}]", request));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] for [{}]", realms, request));
        } else {
            processLogoutResponse(realms.get(0), request, listener);
        }
    }

    private void processLogoutResponse(SamlRealm samlRealm, SamlVerifyLogoutRequest request,
                                       ActionListener<SamlVerifyLogoutResponse> listener) {

        final SamlLogoutResponseHandler logoutResponseHandler = samlRealm.getLogoutResponseHandler();
        try {
            logoutResponseHandler.handle(request.getSaml(), request.getValidRequestIds());
            listener.onResponse(new SamlVerifyLogoutResponse());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
