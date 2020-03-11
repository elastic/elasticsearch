/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.xpack.idp.saml.authn.SuccessfulAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.UserServiceAuthentication;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.opensaml.saml.saml2.core.Response;

import java.io.IOException;
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
        identityProvider.getRegisteredServiceProvider(request.getSpEntityId(), false, ActionListener.wrap(
            sp -> {
                try {
                    if (null == sp) {
                        final String message = "Service Provider with Entity ID [" + request.getSpEntityId()
                            + "] is not registered with this Identity Provider";
                        logger.debug(message);
                        listener.onFailure(new IllegalArgumentException(message));
                        return;
                    }
                    final SecondaryAuthentication secondaryAuthentication = SecondaryAuthentication.readFromContext(securityContext);
                    if (secondaryAuthentication == null) {
                        listener.onFailure(
                            new ElasticsearchSecurityException("Request is missing secondary authentication", RestStatus.FORBIDDEN));
                        return;
                    }
                    buildUserFromAuthentication(secondaryAuthentication, sp, ActionListener.wrap(
                        user -> {
                            if (user == null) {
                                // TODO return SAML failure instead?
                                listener.onFailure(new ElasticsearchSecurityException("User [{}] is not permitted to access service [{}]",
                                    secondaryAuthentication.getUser(), sp));
                                return;
                            }
                            final SuccessfulAuthenticationResponseMessageBuilder builder =
                                new SuccessfulAuthenticationResponseMessageBuilder(samlFactory, Clock.systemUTC(), identityProvider);
                            final Response response = builder.build(user, null);
                            listener.onResponse(new SamlInitiateSingleSignOnResponse(
                                user.getServiceProvider().getAssertionConsumerService().toString(),
                                samlFactory.getXmlContent(response),
                                user.getServiceProvider().getEntityId()));
                        },
                        listener::onFailure
                    ));

                } catch (IOException e) {
                    listener.onFailure(new IllegalArgumentException(e.getMessage()));
                }
            },
            listener::onFailure
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
}
