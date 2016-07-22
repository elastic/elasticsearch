/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.DelegatingTransportChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.ssl.SslHandler;

import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;

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
    void inbound(String action, TransportRequest request, TransportChannel transportChannel) throws IOException;

    /**
     * The server trasnport filter that should be used in nodes as it ensures that an incoming
     * request is properly authenticated and authorized
     */
    class NodeProfile implements ServerTransportFilter {
        private static final ESLogger logger = Loggers.getLogger(NodeProfile.class);

        private final AuthenticationService authcService;
        private final AuthorizationService authzService;
        private final SecurityActionMapper actionMapper;
        private final ThreadContext threadContext;
        private final boolean extractClientCert;

        public NodeProfile(AuthenticationService authcService, AuthorizationService authzService,
                           SecurityActionMapper actionMapper, ThreadContext threadContext, boolean extractClientCert) {
            this.authcService = authcService;
            this.authzService = authzService;
            this.actionMapper = actionMapper;
            this.threadContext = threadContext;
            this.extractClientCert = extractClientCert;
        }

        @Override
        public void inbound(String action, TransportRequest request, TransportChannel transportChannel) throws IOException {
            /*
             here we don't have a fallback user, as all incoming request are
             expected to have a user attached (either in headers or in context)
             We can make this assumption because in nodes we also have the
             {@link ClientTransportFilter.Node} that makes sure all outgoing requsts
             from all the nodes are attached with a user (either a serialize user
             an authentication token
             */
            String securityAction = actionMapper.action(action, request);

            TransportChannel unwrappedChannel = transportChannel;
            while (unwrappedChannel instanceof DelegatingTransportChannel) {
                unwrappedChannel = ((DelegatingTransportChannel) unwrappedChannel).getChannel();
            }

            if (extractClientCert && (unwrappedChannel instanceof TcpTransportChannel)
                    && ((TcpTransportChannel) unwrappedChannel).getChannel() instanceof Channel) {
                Channel channel = (Channel) ((TcpTransportChannel) unwrappedChannel).getChannel();
                SslHandler sslHandler = channel.getPipeline().get(SslHandler.class);
                assert sslHandler != null;

                try {
                    Certificate[] certs = sslHandler.getEngine().getSession().getPeerCertificates();
                    if (certs instanceof X509Certificate[]) {
                        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, certs);
                    }
                } catch (SSLPeerUnverifiedException e) {
                    // this happens when we only request client authentication and the client does not provide it
                    if (logger.isTraceEnabled()) {
                        logger.trace("SSL Peer did not present a certificate on channel [{}]", e, channel);
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("SSL Peer did not present a certificate on channel [{}]", channel);
                    }
                }
            }

            Authentication authentication = authcService.authenticate(securityAction, request, null);
            authzService.authorize(authentication, securityAction, request);
        }
    }

    /**
     * A server transport filter rejects internal calls, which should be used on connections
     * where only clients connect to. This ensures that no client can send any internal actions
     * or shard level actions. As it extends the NodeProfile the authentication/authorization is
     * done as well
     */
    class ClientProfile extends NodeProfile {

        public ClientProfile(AuthenticationService authcService, AuthorizationService authzService,
                             SecurityActionMapper actionMapper, ThreadContext threadContext, boolean extractClientCert) {
            super(authcService, authzService, actionMapper, threadContext, extractClientCert);
        }

        @Override
        public void inbound(String action, TransportRequest request, TransportChannel transportChannel) throws IOException {
            // TODO is ']' sufficient to mark as shard action?
            boolean isInternalOrShardAction = action.startsWith("internal:") || action.endsWith("]");
            if (isInternalOrShardAction) {
                throw authenticationError("executing internal/shard actions is considered malicious and forbidden");
            }
            super.inbound(action, request, transportChannel);
        }
    }

}
