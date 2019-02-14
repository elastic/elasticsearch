/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TaskTransportChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.BwcXPackUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

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
     * The server transport filter that should be used in nodes as it ensures that an incoming
     * request is properly authenticated and authorized
     */
    class NodeProfile implements ServerTransportFilter {
        private static final Logger logger = LogManager.getLogger(NodeProfile.class);

        private final AuthenticationService authcService;
        private final AuthorizationService authzService;
        private final SecurityActionMapper actionMapper = new SecurityActionMapper();
        private final ThreadContext threadContext;
        private final boolean extractClientCert;
        private final DestructiveOperations destructiveOperations;
        private final boolean reservedRealmEnabled;
        private final SecurityContext securityContext;
        private final XPackLicenseState licenseState;

        NodeProfile(AuthenticationService authcService, AuthorizationService authzService,
                    ThreadContext threadContext, boolean extractClientCert, DestructiveOperations destructiveOperations,
                    boolean reservedRealmEnabled, SecurityContext securityContext, XPackLicenseState licenseState) {
            this.authcService = authcService;
            this.authzService = authzService;
            this.threadContext = threadContext;
            this.extractClientCert = extractClientCert;
            this.destructiveOperations = destructiveOperations;
            this.reservedRealmEnabled = reservedRealmEnabled;
            this.securityContext = securityContext;
            this.licenseState = licenseState;
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
                if (authentication != null) {
                    if (reservedRealmEnabled && authentication.getVersion().before(Version.V_5_2_0) &&
                        KibanaUser.NAME.equals(authentication.getUser().authenticatedUser().principal())) {
                        executeAsCurrentVersionKibanaUser(securityAction, request, transportChannel, listener, authentication);
                    } else if (securityAction.equals(TransportService.HANDSHAKE_ACTION_NAME) &&
                               SystemUser.is(authentication.getUser()) == false) {
                        securityContext.executeAsUser(SystemUser.INSTANCE, (ctx) -> {
                            final Authentication replaced = Authentication.getAuthentication(threadContext);
                            authzService.authorize(replaced, securityAction, request, listener);
                        }, version);
                    } else if (authentication.getVersion().before(Version.V_5_6_1) &&
                            XPackUser.NAME.equals(authentication.getUser().authenticatedUser().principal())) {
                        // need to allow old version xpack user since we can get internal operations from a older node
                        // that doesn't know about the xpack security user
                        executeAsOldVersionXPackUser(securityAction, request, transportChannel, listener);
                    } else {
                        authzService.authorize(authentication, securityAction, request, listener);
                    }
                } else if (licenseState.isAuthAllowed() == false) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new IllegalStateException("no authentication present but auth is allowed"));
                }
            }, listener::onFailure));
        }

        private void executeAsCurrentVersionKibanaUser(String securityAction, TransportRequest request, TransportChannel transportChannel,
                                                       ActionListener<Void> listener, Authentication authentication) {
            // the authentication came from an older node - so let's replace the user with our version
            final User kibanaUser = new KibanaUser(authentication.getUser().enabled());
            if (kibanaUser.enabled()) {
                executeAsUser(kibanaUser, securityAction, request, transportChannel, listener);
            } else {
                throw new IllegalStateException("a disabled user should never be sent. " + kibanaUser);
            }
        }

        private void executeAsOldVersionXPackUser(String securityAction, TransportRequest request, TransportChannel transportChannel,
                                                  ActionListener<Void> listener) {
            executeAsUser(BwcXPackUser.INSTANCE, securityAction, request, transportChannel, listener);
        }

        private void executeAsUser(User user, String securityAction, TransportRequest request, TransportChannel transportChannel,
                                   ActionListener<Void> listener) {
            securityContext.executeAsUser(user, (original) -> {
                final Authentication replacedUserAuth = securityContext.getAuthentication();
                authzService.authorize(replacedUserAuth, securityAction, request, listener);
            }, transportChannel.getVersion());
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
                             boolean reservedRealmEnabled, SecurityContext securityContext, XPackLicenseState licenseState) {
            super(authcService, authzService, threadContext, extractClientCert, destructiveOperations, reservedRealmEnabled,
                securityContext, licenseState);
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
