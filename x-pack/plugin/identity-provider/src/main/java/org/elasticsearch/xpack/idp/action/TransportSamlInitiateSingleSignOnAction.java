/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.idp.privileges.UserPrivilegeResolver;
import org.elasticsearch.xpack.idp.saml.authn.FailedAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.SuccessfulAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.UserServiceAuthentication;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlAuthenticationState;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.StatusCode;

import java.time.Clock;

public class TransportSamlInitiateSingleSignOnAction
    extends HandledTransportAction<SamlInitiateSingleSignOnRequest, SamlInitiateSingleSignOnResponse> {

    private final Logger logger = LogManager.getLogger(TransportSamlInitiateSingleSignOnAction.class);

    private final SecurityContext securityContext;
    private final SamlIdentityProvider identityProvider;
    private final SamlFactory samlFactory;
    private final UserPrivilegeResolver privilegeResolver;

    @Inject
    public TransportSamlInitiateSingleSignOnAction(TransportService transportService, ActionFilters actionFilters,
                                                   SecurityContext securityContext, SamlIdentityProvider idp, SamlFactory factory,
                                                   UserPrivilegeResolver privilegeResolver) {
        super(SamlInitiateSingleSignOnAction.NAME, transportService, actionFilters, SamlInitiateSingleSignOnRequest::new);
        this.securityContext = securityContext;
        this.identityProvider = idp;
        this.samlFactory = factory;
        this.privilegeResolver = privilegeResolver;
    }

    @Override
    protected void doExecute(Task task, SamlInitiateSingleSignOnRequest request,
                             ActionListener<SamlInitiateSingleSignOnResponse> listener) {
        final SamlAuthenticationState authenticationState = request.getSamlAuthenticationState();
        identityProvider.resolveServiceProvider(
            request.getSpEntityId(),
            request.getAssertionConsumerService(),
            false,
            ActionListener.wrap(
                sp -> {
                    if (null == sp) {
                        final String message = "Service Provider with Entity ID [" + request.getSpEntityId() + "] and ACS ["
                            + request.getAssertionConsumerService() + "] is not known to this Identity Provider";
                        possiblyReplyWithSamlFailure(authenticationState, request.getSpEntityId(), request.getAssertionConsumerService(),
                            StatusCode.RESPONDER, new IllegalArgumentException(message), listener);
                        return;
                    }
                    final SecondaryAuthentication secondaryAuthentication = SecondaryAuthentication.readFromContext(securityContext);
                    if (secondaryAuthentication == null) {
                        possiblyReplyWithSamlFailure(authenticationState, request.getSpEntityId(), request.getAssertionConsumerService(),
                            StatusCode.REQUESTER,
                            new ElasticsearchSecurityException("Request is missing secondary authentication", RestStatus.FORBIDDEN),
                            listener);
                        return;
                    }
                    buildUserFromAuthentication(secondaryAuthentication, sp, ActionListener.wrap(
                        user -> {
                            if (user == null) {
                                possiblyReplyWithSamlFailure(authenticationState, request.getSpEntityId(),
                                    request.getAssertionConsumerService(), StatusCode.REQUESTER,
                                    new ElasticsearchSecurityException("User [{}] is not permitted to access service [{}]",
                                        RestStatus.FORBIDDEN, secondaryAuthentication.getUser().principal(), sp.getEntityId()),
                                    listener);
                                return;
                            }
                            final SuccessfulAuthenticationResponseMessageBuilder builder =
                                new SuccessfulAuthenticationResponseMessageBuilder(samlFactory, Clock.systemUTC(), identityProvider);
                            try {
                                final Response response = builder.build(user, authenticationState);
                                listener.onResponse(new SamlInitiateSingleSignOnResponse(
                                    user.getServiceProvider().getEntityId(),
                                    user.getServiceProvider().getAssertionConsumerService().toString(),
                                    samlFactory.getXmlContent(response),
                                    StatusCode.SUCCESS,
                                    null));
                            } catch (ElasticsearchException e) {
                                listener.onFailure(e);
                            }
                        },
                        e -> possiblyReplyWithSamlFailure(authenticationState, request.getSpEntityId(),
                            request.getAssertionConsumerService(), StatusCode.RESPONDER, e, listener)
                    ));
                },
                e -> possiblyReplyWithSamlFailure(authenticationState, request.getSpEntityId(), request.getAssertionConsumerService(),
                    StatusCode.RESPONDER, e, listener)
            ));
    }

    private void buildUserFromAuthentication(SecondaryAuthentication secondaryAuthentication, SamlServiceProvider serviceProvider,
                                             ActionListener<UserServiceAuthentication> listener) {
        User user = secondaryAuthentication.getUser();
        secondaryAuthentication.execute(ignore -> {
                privilegeResolver.resolve(serviceProvider.getPrivileges(), ActionListener.wrap(
                    userPrivileges -> {
                        if (userPrivileges.hasAccess == false) {
                            listener.onResponse(null);
                        } else {
                            logger.debug("Resolved [{}] for [{}]", userPrivileges, user);
                            listener.onResponse(new UserServiceAuthentication(user.principal(), user.fullName(), user.email(),
                                userPrivileges.roles, serviceProvider));
                        }
                    },
                    listener::onFailure
                ));
                return null;
            }
        );
    }

    private void possiblyReplyWithSamlFailure(SamlAuthenticationState authenticationState, String spEntityId,
                                              String acsUrl, String statusCode, Exception e,
                                              ActionListener<SamlInitiateSingleSignOnResponse> listener) {
        logger.debug("Failed to generate a successful SAML response: ", e);
        if (authenticationState != null) {
            final FailedAuthenticationResponseMessageBuilder builder =
                new FailedAuthenticationResponseMessageBuilder(samlFactory, Clock.systemUTC(), identityProvider)
                    .setInResponseTo(authenticationState.getAuthnRequestId())
                    .setAcsUrl(acsUrl)
                    .setPrimaryStatusCode(statusCode);
            final Response response = builder.build();
            listener.onResponse(new SamlInitiateSingleSignOnResponse(
                spEntityId,
                acsUrl,
                samlFactory.getXmlContent(response),
                statusCode,
                e.getMessage()));
        } else {
            listener.onFailure(e);
        }
    }
}
