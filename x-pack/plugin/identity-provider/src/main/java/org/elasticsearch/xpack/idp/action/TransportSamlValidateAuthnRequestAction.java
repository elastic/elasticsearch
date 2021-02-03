/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.authn.SamlAuthnRequestValidator;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;

public class TransportSamlValidateAuthnRequestAction
    extends HandledTransportAction<SamlValidateAuthnRequestRequest, SamlValidateAuthnRequestResponse> {

    private final SamlIdentityProvider identityProvider;
    private final SamlFactory samlFactory;

    @Inject
    public TransportSamlValidateAuthnRequestAction(TransportService transportService, ActionFilters actionFilters,
                                                   SamlIdentityProvider idp, SamlFactory factory) {
        super(SamlValidateAuthnRequestAction.NAME, transportService, actionFilters, SamlValidateAuthnRequestRequest::new);
        this.identityProvider = idp;
        this.samlFactory = factory;
    }

    @Override
    protected void doExecute(Task task, SamlValidateAuthnRequestRequest request,
                             ActionListener<SamlValidateAuthnRequestResponse> listener) {
        final SamlAuthnRequestValidator validator = new SamlAuthnRequestValidator(samlFactory, identityProvider);
        try {
            validator.processQueryString(request.getQueryString(), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
