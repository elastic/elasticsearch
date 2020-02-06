/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.authn.SamlAuthnRequestValidator;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;

public class TransportSamlValidateAuthnRequestAction extends HandledTransportAction<SamlValidateAuthnRequestRequest,
    SamlValidateAuthnRequestResponse> {

    private final Environment env;
    private final Logger logger = LogManager.getLogger(TransportSamlValidateAuthnRequestAction.class);

    @Inject
    public TransportSamlValidateAuthnRequestAction(TransportService transportService, ActionFilters actionFilters,
                                                   Environment environment) {
        super(SamlValidateAuthnRequestAction.NAME, transportService, actionFilters, SamlValidateAuthnRequestRequest::new);
        this.env = environment;
    }

    @Override
    protected void doExecute(Task task, SamlValidateAuthnRequestRequest request,
                             ActionListener<SamlValidateAuthnRequestResponse> listener) {
        final SamlIdentityProvider idp = new CloudIdp(env, env.settings());
        final SamlAuthnRequestValidator validator = new SamlAuthnRequestValidator(idp);
        try {
            final SamlValidateAuthnRequestResponse response = validator.processQueryString(request.getQueryString());
            logger.trace(new ParameterizedMessage("Validated AuthnResponse from queryString [{}] and extracted [{}]",
                request.getQueryString(), response));
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
