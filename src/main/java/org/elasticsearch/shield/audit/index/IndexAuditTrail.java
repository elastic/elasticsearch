/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.rest.RemoteHostHeader;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;

import static org.elasticsearch.shield.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.shield.audit.AuditUtil.indices;

/**
 * Audit trail implementation that writes events into an index.
 */
public class IndexAuditTrail implements AuditTrail {

    private static final ESLogger logger = ESLoggerFactory.getLogger(IndexAuditTrail.class.getName());

    public static final String NAME = "index";

    private final String nodeName;
    private final String nodeHostName;
    private final String nodeHostAddress;
    private final IndexAuditUserHolder auditUser;
    private final IndexAuditTrailBulkProcessor processor;

    @Override
    public String name() {
        return NAME;
    }

    private enum Level {
        ANONYMOUS_ACCESS_DENIED,
        AUTHENTICATION_FAILED,
        ACCESS_GRANTED,
        ACCESS_DENIED,
        TAMPERED_REQUEST,
        CONNECTION_GRANTED,
        CONNECTION_DENIED,
        SYSTEM_ACCESS_GRANTED;
    }

    private final EnumSet<Level> enabled = EnumSet.allOf(Level.class);

    @Inject
    public IndexAuditTrail(Settings settings, IndexAuditUserHolder indexingAuditUser,
                           IndexAuditTrailBulkProcessor processor) {

        this.auditUser = indexingAuditUser;
        this.processor = processor;
        this.nodeName = settings.get("name");

        String hostname = "n/a";
        String hostaddr = "n/a";

        try {
            hostname = InetAddress.getLocalHost().getHostName();
            hostaddr = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("unable to resolve local host name", e);
        }
        this.nodeHostName = hostname;
        this.nodeHostAddress = hostaddr;

        if (!settings.getAsBoolean("shield.audit.index.events.system.access_granted", false)) {
            enabled.remove(Level.SYSTEM_ACCESS_GRANTED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.anonymous_access_denied", true)) {
            enabled.remove(Level.ANONYMOUS_ACCESS_DENIED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.authentication_failed", true)) {
            enabled.remove(Level.AUTHENTICATION_FAILED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.access_granted", true)) {
            enabled.remove(Level.ACCESS_GRANTED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.access_denied", true)) {
            enabled.remove(Level.ACCESS_DENIED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.tampered_request", true)) {
            enabled.remove(Level.TAMPERED_REQUEST);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.connection_granted", true)) {
            enabled.remove(Level.CONNECTION_GRANTED);
        }
        if (!settings.getAsBoolean("shield.audit.index.events.connection_denied", true)) {
            enabled.remove(Level.CONNECTION_DENIED);
        }
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage<?> message) {
        if (enabled.contains(Level.ANONYMOUS_ACCESS_DENIED)) {
            try {
                processor.submit(message("anonymous_access_denied", action, null, null, indices(message), message));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (enabled.contains(Level.ANONYMOUS_ACCESS_DENIED)) {
            try {
                processor.submit(message("anonymous_access_denied", null, null, null, null, request));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        if (enabled.contains(Level.AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    processor.submit(message("authentication_failed", action, token.principal(), null, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (enabled.contains(Level.AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    processor.submit(message("authentication_failed", null, token.principal(), null, null, request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (enabled.contains(Level.AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    processor.submit(message("authentication_failed", action, token.principal(), realm, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (enabled.contains(Level.AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    processor.submit(message("authentication_failed", null, token.principal(), realm, null, request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        if (enabled.contains(Level.ACCESS_GRANTED)) {
            if (!principalIsAuditor(user.principal())) {
                // special treatment for internal system actions - only log if explicitly told to
                if (Privilege.SYSTEM.internalActionPredicate().apply(action)) {
                    if (enabled.contains(Level.SYSTEM_ACCESS_GRANTED)) {
                        try {
                            processor.submit(message("access_granted", action, user.principal(), null, indices(message), message));
                        } catch (Exception e) {
                            logger.warn("failed to index audit event: [access_granted]", e);
                        }
                    }
                } else {
                    try {
                        processor.submit(message("access_granted", action, user.principal(), null, indices(message), message));
                    } catch (Exception e) {
                        logger.warn("failed to index audit event: [access_granted]", e);
                    }
                }
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        if (enabled.contains(Level.ACCESS_DENIED)) {
            if (!principalIsAuditor(user.principal())) {
                try {
                    processor.submit(message("access_denied", action, user.principal(), null, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_denied]", e);
                }
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        if (enabled.contains(Level.TAMPERED_REQUEST)) {
            if (!principalIsAuditor(user.principal())) {
                try {
                    processor.submit(message("tampered_request", action, user.principal(), null, indices(request), request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [tampered_request]", e);
                }
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (enabled.contains(Level.CONNECTION_GRANTED)) {
            try {
                processor.submit(message("ip_filter", "connection_granted", inetAddress, profile, rule));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_granted]", e);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (enabled.contains(Level.CONNECTION_DENIED)) {
            try {
                processor.submit(message("ip_filter", "connection_denied", inetAddress, profile, rule));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_denied]", e);
            }
        }
    }

    private boolean principalIsAuditor(String principal) {
        return (principal.equals(auditUser.user().principal()));
    }

    private Message message(String type, @Nullable String action, @Nullable String principal,
                            @Nullable String realm, @Nullable String indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (principal != null) {
            msg.builder.field(Field.PRINCIPAL, principal);
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.field(Field.INDICES, indices);
        }
        if (logger.isDebugEnabled()) {
            msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());
        }

        return msg.end();
    }

    private Message message(String type, @Nullable String action, @Nullable String principal,
                            @Nullable String realm, @Nullable String indices, RestRequest request) throws Exception {

        Message msg = new Message().start();
        common("rest", type, msg.builder);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (principal != null) {
            msg.builder.field(Field.PRINCIPAL, principal);
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.field(Field.INDICES, indices);
        }
        if (logger.isDebugEnabled()) {
            msg.builder.field(Field.REQUEST_BODY, restRequestContent(request));
        }

        msg.builder.field(Field.ORIGIN_ADDRESS, request.getRemoteAddress());
        msg.builder.field(Field.URI, request.uri());

        return msg.end();
    }

    private Message message(String layer, String type, InetAddress originAddress, String profile,
                            ShieldIpFilterRule rule) throws IOException {

        Message msg = new Message().start();
        common(layer, type, msg.builder);

        msg.builder.field(Field.ORIGIN_ADDRESS, originAddress.getHostAddress());
        msg.builder.field(Field.TRANSPORT_PROFILE, profile);
        msg.builder.field(Field.RULE, rule);

        return msg.end();
    }

    private XContentBuilder common(String layer, String type, XContentBuilder builder) throws IOException {
        builder.field(Field.NODE_NAME, nodeName);
        builder.field(Field.NODE_HOST_NAME, nodeHostName);
        builder.field(Field.NODE_HOST_ADDRESS, nodeHostAddress);
        builder.field(Field.LAYER, layer);
        builder.field(Field.TYPE, type);
        return builder;
    }

    private static XContentBuilder originAttributes(TransportMessage message, XContentBuilder builder) throws IOException {

        // first checking if the message originated in a rest call
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(message);
        if (restAddress != null) {
            builder.field(Field.ORIGIN_TYPE, "rest");
            builder.field(Field.ORIGIN_ADDRESS, restAddress);
            return builder;
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.field(Field.ORIGIN_TYPE, "transport");
            if (address instanceof InetSocketTransportAddress) {
                builder.field(Field.ORIGIN_ADDRESS, ((InetSocketTransportAddress) address).address());
            } else {
                builder.field(Field.ORIGIN_ADDRESS, address);
            }
            return builder;
        }

        // the call was originated locally on this node
        builder.field(Field.ORIGIN_TYPE, "local_node");
        builder.field(Field.ORIGIN_ADDRESS, NetworkUtils.getLocalHostAddress("_local"));
        return builder;
    }

    static class Message {

        final long timestamp;
        final XContentBuilder builder;

        Message() throws IOException {
            this.timestamp = System.currentTimeMillis();
            this.builder = XContentFactory.jsonBuilder();
        }

        Message start() throws IOException {
            builder.startObject();
            builder.field(Field.TIMESTAMP, timestamp);
            return this;
        }

        Message end() throws IOException {
            builder.endObject();
            return this;
        }
    }

    interface Field {
        XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        XContentBuilderString NODE_NAME = new XContentBuilderString("node_name");
        XContentBuilderString NODE_HOST_NAME = new XContentBuilderString("node_host_name");
        XContentBuilderString NODE_HOST_ADDRESS = new XContentBuilderString("node_host_address");
        XContentBuilderString LAYER = new XContentBuilderString("layer");
        XContentBuilderString TYPE = new XContentBuilderString("type");
        XContentBuilderString ORIGIN_ADDRESS = new XContentBuilderString("origin_address");
        XContentBuilderString ORIGIN_TYPE = new XContentBuilderString("origin_type");
        XContentBuilderString PRINCIPAL = new XContentBuilderString("principal");
        XContentBuilderString ACTION = new XContentBuilderString("action");
        XContentBuilderString INDICES = new XContentBuilderString("indices");
        XContentBuilderString REQUEST = new XContentBuilderString("request");
        XContentBuilderString REQUEST_BODY = new XContentBuilderString("request_body");
        XContentBuilderString URI = new XContentBuilderString("uri");
        XContentBuilderString REALM = new XContentBuilderString("realm");
        XContentBuilderString TRANSPORT_PROFILE = new XContentBuilderString("transport_profile");
        XContentBuilderString RULE = new XContentBuilderString("rule");
    }
}
