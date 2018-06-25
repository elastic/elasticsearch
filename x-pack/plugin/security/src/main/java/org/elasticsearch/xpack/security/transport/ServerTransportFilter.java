/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TaskTransportChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * This interface allows to intercept messages as they come in and execute logic
 * This is used in x-pack security to execute the authentication/authorization on incoming
 * messages.
 * Note that this filter only applies for nodes, but not for clients.
 */
public interface ServerTransportFilter {

    /**
     * Called just after the given request was received by the transport. Any exception
     * thrown by this method will stop the request from being handled and the error will
     * be sent back to the sender.
     */
    void inbound(String action, TransportRequest request, TransportChannel transportChannel, ActionListener<Void> listener)
            throws IOException;

    /**
     * The server trasnport filter that should be used in nodes as it ensures that an incoming
     * request is properly authenticated and authorized
     */
    class NodeProfile implements ServerTransportFilter {
        private static final Logger logger = Loggers.getLogger(NodeProfile.class);

        private final AuthenticationService authcService;
        private final AuthorizationService authzService;
        private final SecurityActionMapper actionMapper = new SecurityActionMapper();
        private final ThreadContext threadContext;
        private final boolean extractClientCert;
        private final DestructiveOperations destructiveOperations;
        private final boolean reservedRealmEnabled;
        private final SecurityContext securityContext;

        NodeProfile(AuthenticationService authcService, AuthorizationService authzService,
                    ThreadContext threadContext, boolean extractClientCert, DestructiveOperations destructiveOperations,
                    boolean reservedRealmEnabled, SecurityContext securityContext) {
            this.authcService = authcService;
            this.authzService = authzService;
            this.threadContext = threadContext;
            this.extractClientCert = extractClientCert;
            this.destructiveOperations = destructiveOperations;
            this.reservedRealmEnabled = reservedRealmEnabled;
            this.securityContext = securityContext;
        }

        @Override
        public void inbound(String action, TransportRequest request, TransportChannel transportChannel, ActionListener<Void> listener)
                throws IOException {
            if (CloseIndexAction.NAME.equals(action) || OpenIndexAction.NAME.equals(action) || DeleteIndexAction.NAME.equals(action)) {
                IndicesRequest indicesRequest = (IndicesRequest) request;
                try {
                    destructiveOperations.failDestructive(indicesRequest.indices());
                } catch(IllegalArgumentException e) {
                    listener.onFailure(e);
                    return;
                }
            }
            /*
             here we don't have a fallback user, as all incoming request are
             expected to have a user attached (either in headers or in context)
             We can make this assumption because in nodes we make sure all outgoing
             requests from all the nodes are attached with a user (either a serialize
             user an authentication token
             */
            String securityAction = actionMapper.action(action, request);

            TransportChannel unwrappedChannel = transportChannel;
            if (unwrappedChannel instanceof TaskTransportChannel) {
                unwrappedChannel = ((TaskTransportChannel) unwrappedChannel).getChannel();
            }

            if (extractClientCert && (unwrappedChannel instanceof TcpTransportChannel) &&
                ((TcpTransportChannel) unwrappedChannel).getChannel() instanceof Netty4TcpChannel) {
                Channel channel = ((Netty4TcpChannel) ((TcpTransportChannel) unwrappedChannel).getChannel()).getLowLevelChannel();
                SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
                if (channel.isOpen()) {
                    assert sslHandler != null : "channel [" + channel + "] did not have a ssl handler. pipeline " + channel.pipeline();
                    extractClientCertificates(logger, threadContext, sslHandler.engine(), channel);
                }
            }

            final Version version = transportChannel.getVersion().equals(Version.V_5_4_0) ? Version.CURRENT : transportChannel.getVersion();
            authcService.authenticate(securityAction, request, (User)null, ActionListener.wrap((authentication) -> {
                    if (reservedRealmEnabled && authentication.getVersion().before(Version.V_5_2_0) &&
                        KibanaUser.NAME.equals(authentication.getUser().authenticatedUser().principal())) {
                        executeAsCurrentVersionKibanaUser(securityAction, request, transportChannel, listener, authentication);
                    } else if (securityAction.equals(TransportService.HANDSHAKE_ACTION_NAME) &&
                               SystemUser.is(authentication.getUser()) == false) {
                        securityContext.executeAsUser(SystemUser.INSTANCE, (ctx) -> {
                            final Authentication replaced = Authentication.getAuthentication(threadContext);
                            final AuthorizationUtils.AsyncAuthorizer asyncAuthorizer =
                                    new AuthorizationUtils.AsyncAuthorizer(replaced, listener, (userRoles, runAsRoles) -> {
                                        authzService.authorize(replaced, securityAction, request, userRoles, runAsRoles);
                                        listener.onResponse(null);
                                    });
                            asyncAuthorizer.authorize(authzService);
                        }, version);
                    } else {
                        final AuthorizationUtils.AsyncAuthorizer asyncAuthorizer =
                                new AuthorizationUtils.AsyncAuthorizer(authentication, listener, (userRoles, runAsRoles) -> {
                                    authzService.authorize(authentication, securityAction, request, userRoles, runAsRoles);
                                    listener.onResponse(null);
                                });
                        asyncAuthorizer.authorize(authzService);
                    }
                }, listener::onFailure));
        }

        private void executeAsCurrentVersionKibanaUser(String securityAction, TransportRequest request, TransportChannel transportChannel,
                                                       ActionListener<Void> listener, Authentication authentication) {
            // the authentication came from an older node - so let's replace the user with our version
            final User kibanaUser = new KibanaUser(authentication.getUser().enabled());
            if (kibanaUser.enabled()) {
                securityContext.executeAsUser(kibanaUser, (original) -> {
                    final Authentication replacedUserAuth = securityContext.getAuthentication();
                    final AuthorizationUtils.AsyncAuthorizer asyncAuthorizer =
                        new AuthorizationUtils.AsyncAuthorizer(replacedUserAuth, listener, (userRoles, runAsRoles) -> {
                            authzService.authorize(replacedUserAuth, securityAction, request, userRoles, runAsRoles);
                            listener.onResponse(null);
                        });
                    asyncAuthorizer.authorize(authzService);
                }, transportChannel.getVersion());
            } else {
                throw new IllegalStateException("a disabled user should never be sent. " + kibanaUser);
            }
        }
    }

    static void extractClientCertificates(Logger logger, ThreadContext threadContext, SSLEngine sslEngine, Channel channel) {
        try {
            Certificate[] certs = sslEngine.getSession().getPeerCertificates();
            if (certs instanceof X509Certificate[]) {
                threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, certs);
            }
        } catch (SSLPeerUnverifiedException e) {
            // this happens when client authentication is optional and the client does not provide credentials. If client
            // authentication was required then this connection should be closed before ever getting into this class
            assert sslEngine.getNeedClientAuth() == false;
            assert sslEngine.getWantClientAuth();
            if (logger.isTraceEnabled()) {
                logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "SSL Peer did not present a certificate on channel [{}]", channel), e);
            } else if (logger.isDebugEnabled()) {
                logger.debug("SSL Peer did not present a certificate on channel [{}]", channel);
            }
        }
    }

    /**
     * A server transport filter rejects internal calls, which should be used on connections
     * where only clients connect to. This ensures that no client can send any internal actions
     * or shard level actions. As it extends the NodeProfile the authentication/authorization is
     * done as well
     */
    class ClientProfile extends NodeProfile {

        ClientProfile(AuthenticationService authcService, AuthorizationService authzService,
                             ThreadContext threadContext, boolean extractClientCert, DestructiveOperations destructiveOperations,
                             boolean reservedRealmEnabled, SecurityContext securityContext) {
            super(authcService, authzService, threadContext, extractClientCert, destructiveOperations, reservedRealmEnabled,
                    securityContext);
        }

        @Override
        public void inbound(String action, TransportRequest request, TransportChannel transportChannel, ActionListener<Void> listener)
                throws IOException {
            // TODO is ']' sufficient to mark as shard action?
            final boolean isInternalOrShardAction = action.startsWith("internal:") || action.endsWith("]");
            if (isInternalOrShardAction && TransportService.HANDSHAKE_ACTION_NAME.equals(action) == false) {
                throw authenticationError("executing internal/shard actions is considered malicious and forbidden");
            }
            super.inbound(action, request, transportChannel, listener);
        }
    }

}
