/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.idp.saml.authn.SuccessfulAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.UserServiceAuthentication;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.opensaml.saml.saml2.core.Response;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TransportSamlInitiateSingleSignOnAction
    extends HandledTransportAction<SamlInitiateSingleSignOnRequest, SamlInitiateSingleSignOnResponse> {

    private final Logger logger = LogManager.getLogger(TransportSamlInitiateSingleSignOnAction.class);
    private final SecurityContext securityContext;
    private final SamlIdentityProvider identityProvider;
    private final SamlFactory samlFactory;

    @Inject
    public TransportSamlInitiateSingleSignOnAction(TransportService transportService, ActionFilters actionFilters,
                                                   SecurityContext securityContext, SamlIdentityProvider idp, SamlFactory factory) {
        super(SamlInitiateSingleSignOnAction.NAME, transportService, actionFilters, SamlInitiateSingleSignOnRequest::new);
        this.securityContext = securityContext;
        this.identityProvider = idp;
        this.samlFactory = factory;
    }

    @Override
    protected void doExecute(Task task, SamlInitiateSingleSignOnRequest request,
                             ActionListener<SamlInitiateSingleSignOnResponse> listener) {
        try {
            final SamlServiceProvider sp = identityProvider.getRegisteredServiceProvider(request.getSpEntityId());
            if (null == sp) {
                final String message =
                    "Service Provider with Entity ID [" + request.getSpEntityId() + "] is not registered with this Identity Provider";
                logger.debug(message);
                listener.onFailure(new IllegalArgumentException(message));
                return;
            }
            final SecondaryAuthentication secondaryAuthentication = SecondaryAuthentication.readFromContext(securityContext);
            if (secondaryAuthentication == null) {
                listener.onFailure(new IllegalStateException("Request is missing secondary authentication"));
                return;
            }
            final UserServiceAuthentication user = buildUserFromAuthentication(secondaryAuthentication.getAuthentication(), sp);
            final SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(samlFactory,
                Clock.systemUTC(), identityProvider);
            final Response response = builder.build(user, null);
            listener.onResponse(new SamlInitiateSingleSignOnResponse(user.getServiceProvider().getAssertionConsumerService().toString(),
                samlFactory.getXmlContent(response),
                user.getServiceProvider().getEntityId()));
        } catch (IOException e) {
            listener.onFailure(new IllegalArgumentException(e.getMessage()));
        }
    }

    private UserServiceAuthentication buildUserFromAuthentication(Authentication authentication, SamlServiceProvider sp) {
        final User authenticatedUser = authentication.getUser();
        final Set<String> groups = new HashSet<>(Arrays.asList(authenticatedUser.roles()));
        return new UserServiceAuthentication(authenticatedUser.principal(), groups, sp);
    }
}
