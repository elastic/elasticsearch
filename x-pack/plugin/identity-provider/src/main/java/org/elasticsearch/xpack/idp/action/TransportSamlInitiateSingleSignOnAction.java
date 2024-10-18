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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
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
import org.elasticsearch.xpack.idp.saml.support.SamlInitiateSingleSignOnException;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.StatusCode;

import java.time.Clock;

public class TransportSamlInitiateSingleSignOnAction extends HandledTransportAction<
    SamlInitiateSingleSignOnRequest,
    SamlInitiateSingleSignOnResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSamlInitiateSingleSignOnAction.class);

    private final SecurityContext securityContext;
    private final SamlIdentityProvider identityProvider;
    private final SamlFactory samlFactory;
    private final UserPrivilegeResolver privilegeResolver;

    @Inject
    public TransportSamlInitiateSingleSignOnAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        SamlIdentityProvider idp,
        SamlFactory factory,
        UserPrivilegeResolver privilegeResolver
    ) {
        super(
            SamlInitiateSingleSignOnAction.NAME,
            transportService,
            actionFilters,
            SamlInitiateSingleSignOnRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.securityContext = securityContext;
        this.identityProvider = idp;
        this.samlFactory = factory;
        this.privilegeResolver = privilegeResolver;
    }

    @Override
    protected void doExecute(
        Task task,
        SamlInitiateSingleSignOnRequest request,
        ActionListener<SamlInitiateSingleSignOnResponse> listener
    ) {
        final SamlAuthenticationState authenticationState = request.getSamlAuthenticationState();
        identityProvider.resolveServiceProvider(
            request.getSpEntityId(),
            request.getAssertionConsumerService(),
            false,
            ActionListener.wrap(sp -> {
                if (null == sp) {
                    writeFailureResponse(
                        listener,
                        buildSamlInitiateSingleSignOnException(
                            authenticationState,
                            request.getSpEntityId(),
                            request.getAssertionConsumerService(),
                            StatusCode.RESPONDER,
                            RestStatus.BAD_REQUEST,
                            "Service Provider with Entity ID [{}] and ACS [{}] is not known to this Identity Provider",
                            null,
                            request.getSpEntityId(),
                            request.getAssertionConsumerService()
                        )

                    );
                    return;
                }
                final SecondaryAuthentication secondaryAuthentication = SecondaryAuthentication.readFromContext(securityContext);
                if (secondaryAuthentication == null) {
                    writeFailureResponse(
                        listener,
                        buildSamlInitiateSingleSignOnException(
                            authenticationState,
                            request.getSpEntityId(),
                            request.getAssertionConsumerService(),
                            StatusCode.REQUESTER,
                            RestStatus.FORBIDDEN,
                            "Request is missing secondary authentication",
                            null
                        )
                    );
                    return;
                }
                buildUserFromAuthentication(secondaryAuthentication, sp, ActionListener.wrap(user -> {
                    if (user == null) {
                        writeFailureResponse(
                            listener,
                            buildSamlInitiateSingleSignOnException(
                                authenticationState,
                                request.getSpEntityId(),
                                request.getAssertionConsumerService(),
                                StatusCode.REQUESTER,
                                RestStatus.FORBIDDEN,
                                "User [{}] is not permitted to access service [{}]",
                                null,
                                secondaryAuthentication.getUser().principal(),
                                sp.getEntityId()
                            )
                        );
                        return;
                    }
                    final SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(
                        samlFactory,
                        Clock.systemUTC(),
                        identityProvider
                    );
                    try {
                        final Response response = builder.build(user, authenticationState);
                        listener.onResponse(
                            new SamlInitiateSingleSignOnResponse(
                                user.getServiceProvider().getEntityId(),
                                user.getServiceProvider().getAssertionConsumerService().toString(),
                                samlFactory.getXmlContent(response),
                                StatusCode.SUCCESS,
                                null
                            )
                        );
                    } catch (ElasticsearchException e) {
                        listener.onFailure(e);
                    }
                },
                    e -> writeFailureResponse(
                        listener,
                        buildResponderSamlInitiateSingleSignOnException(
                            authenticationState,
                            request.getSpEntityId(),
                            request.getAssertionConsumerService(),
                            e
                        )
                    )
                ));
            },
                e -> writeFailureResponse(
                    listener,
                    buildResponderSamlInitiateSingleSignOnException(
                        authenticationState,
                        request.getSpEntityId(),
                        request.getAssertionConsumerService(),
                        e
                    )
                )
            )
        );
    }

    private void buildUserFromAuthentication(
        SecondaryAuthentication secondaryAuthentication,
        SamlServiceProvider serviceProvider,
        ActionListener<UserServiceAuthentication> listener
    ) {
        User user = secondaryAuthentication.getUser();
        secondaryAuthentication.execute(ignore -> {
            ActionListener<UserPrivilegeResolver.UserPrivileges> wrapped = listener.delegateFailureAndWrap((delegate, userPrivileges) -> {
                if (userPrivileges.hasAccess == false) {
                    delegate.onResponse(null);
                } else {
                    logger.debug("Resolved [{}] for [{}]", userPrivileges, user);
                    delegate.onResponse(
                        new UserServiceAuthentication(
                            user.principal(),
                            user.fullName(),
                            user.email(),
                            userPrivileges.roles,
                            serviceProvider
                        )
                    );
                }
            });
            privilegeResolver.resolve(serviceProvider.getPrivileges(), wrapped);
            return null;
        });
    }

    private void writeFailureResponse(
        final ActionListener<SamlInitiateSingleSignOnResponse> listener,
        final SamlInitiateSingleSignOnException ex
    ) {
        logger.debug("Failed to generate a successful SAML response: ", ex);
        listener.onFailure(ex);
    }

    private SamlInitiateSingleSignOnException buildSamlInitiateSingleSignOnException(
        final SamlAuthenticationState authenticationState,
        final String spEntityId,
        final String acsUrl,
        final String statusCode,
        final RestStatus restStatus,
        final String messageFormatStr,
        final Exception cause,
        final Object... args
    ) {
        final SamlInitiateSingleSignOnException ex;
        String exceptionMessage = LoggerMessageFormat.format(messageFormatStr, args);
        if (authenticationState != null) {
            final FailedAuthenticationResponseMessageBuilder builder = new FailedAuthenticationResponseMessageBuilder(
                samlFactory,
                Clock.systemUTC(),
                identityProvider
            ).setInResponseTo(authenticationState.getAuthnRequestId()).setAcsUrl(acsUrl).setPrimaryStatusCode(statusCode);
            final Response response = builder.build();
            ex = new SamlInitiateSingleSignOnException(
                exceptionMessage,
                restStatus,
                cause,
                new SamlInitiateSingleSignOnResponse(spEntityId, acsUrl, samlFactory.getXmlContent(response), statusCode, exceptionMessage)
            );
        } else {
            ex = new SamlInitiateSingleSignOnException(exceptionMessage, restStatus, cause);
        }
        return ex;
    }

    private SamlInitiateSingleSignOnException buildResponderSamlInitiateSingleSignOnException(
        final SamlAuthenticationState authenticationState,
        final String spEntityId,
        final String acsUrl,
        final Exception cause
    ) {
        final String exceptionMessage = cause.getMessage();
        final RestStatus restStatus = ExceptionsHelper.status(cause);
        return buildSamlInitiateSingleSignOnException(
            authenticationState,
            spEntityId,
            acsUrl,
            StatusCode.RESPONDER,
            restStatus,
            exceptionMessage,
            cause
        );
    }
}
