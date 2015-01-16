/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 */
public class LoggingAuditTrail implements AuditTrail {

    public static final String NAME = "logfile";

    private final String prefix;
    private final ESLogger logger;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public LoggingAuditTrail(Settings settings) {
        this(resolvePrefix(settings), Loggers.getLogger(LoggingAuditTrail.class));
    }

    LoggingAuditTrail(Settings settings, ESLogger logger) {
        this(resolvePrefix(settings), logger);
    }

    LoggingAuditTrail(ESLogger logger) {
        this("", logger);
    }

    LoggingAuditTrail(String prefix, ESLogger logger) {
        this.logger = logger;
        this.prefix = prefix;
    }

    @Override
    public void anonymousAccess(String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ANONYMOUS_ACCESS\thost=[{}], action=[{}], indices=[{}], request=[{}]", prefix, message.remoteAddress(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.warn("{}ANONYMOUS_ACCESS\thost=[{}], action=[{}], indices=[{}]", prefix, message.remoteAddress(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ANONYMOUS_ACCESS\thost=[{}], action=[{}], request=[{}]", prefix, message.remoteAddress(), action, message.getClass().getSimpleName());
            } else {
                logger.warn("{}ANONYMOUS_ACCESS\thost=[{}], action=[{}]", prefix, message.remoteAddress(), action);
            }
        }
    }

    @Override
    public void anonymousAccess(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}ANONYMOUS_ACCESS\thost=[{}], URI=[{}], request=[{}]", prefix, request.getRemoteAddress(), request.uri(), restRequestContent(request));
        } else {
            logger.warn("{}ANONYMOUS_ACCESS\thost=[{}], URI=[{}]", prefix, request.getRemoteAddress(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, message.remoteAddress(), token.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.error("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", prefix, message.remoteAddress(), token.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, message.remoteAddress(), token.principal(), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}]", prefix, message.remoteAddress(), token.principal(), action);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], URI=[{}], request=[{}]", prefix, request.getRemoteAddress(), token.principal(), request.uri(), restRequestContent(request));
        } else {
            logger.error("{}AUTHENTICATION_FAILED\thost=[{}], principal=[{}], URI=[{}]", prefix, request.getRemoteAddress(), token.principal(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (logger.isTraceEnabled()) {
            String indices = indices(message);
            if (indices != null) {
                logger.trace("{}AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, realm, message.remoteAddress(), token.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.trace("{}AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, realm, message.remoteAddress(), token.principal(), action, message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], URI=[{}], request=[{}]", prefix, realm, request.getRemoteAddress(), token.principal(), request.uri(), restRequestContent(request));
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        String indices = indices(message);

        // special treatment for internal system actions - only log on trace
        if (Privilege.SYSTEM.internalActionPredicate().apply(action)) {
            if (logger.isTraceEnabled()) {
                if (indices != null) {
                    logger.trace("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, indices, message.getClass().getSimpleName());
                } else {
                    logger.trace("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, message.getClass().getSimpleName());
                }
            }
            return;
        }

        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.info("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", prefix, message.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, message.getClass().getSimpleName());
            } else {
                logger.info("{}ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}]", prefix, message.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.error("{}ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", prefix, message.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, message.remoteAddress(), user.principal(), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}]", prefix, message.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        String indices = indices(request);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", prefix, request.remoteAddress(), user.principal(), action, indices, request.getClass().getSimpleName());
            } else {
                logger.error("{}TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], indices=[{}]", prefix, request.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], request=[{}]", prefix, request.remoteAddress(), user.principal(), action, request.getClass().getSimpleName());
            } else {
                logger.error("{}TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}]", prefix, request.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}CONNECTION_GRANTED\thost=[{}], profile=[{}], rule=[{}]", prefix, inetAddress.getHostAddress(), profile, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        logger.error("{}CONNECTION_DENIED\thost=[{}], profile=[{}], rule=[{}]", prefix, inetAddress.getHostAddress(), profile, rule);
    }

    private static String indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return Strings.arrayToCommaDelimitedString(((IndicesRequest) message).indices());
        }
        return null;
    }

    private static String restRequestContent(RestRequest request) {
        if (request.hasContent()) {
            try {
                return XContentHelper.convertToJson(request.content(), false, false);
            } catch (IOException ioe) {
                return "Invalid Format: " + request.content().toUtf8();
            }
        }
        return "";
    }

    static String resolvePrefix(Settings settings) {
        StringBuilder builder = new StringBuilder();
        if (settings.getAsBoolean("shield.audit.logfile.prefix.node_host_address", false)) {
            try {
                String address = InetAddress.getLocalHost().getHostAddress();
                builder.append("[").append(address).append("] ");
            } catch (UnknownHostException e) {
                // ignore
            }
        }
        if (settings.getAsBoolean("shield.audit.logfile.prefix.node_host_name", false)) {
            try {
                String hostName = InetAddress.getLocalHost().getHostName();
                builder.append("[").append(hostName).append("] ");
            } catch (UnknownHostException e) {
                // ignore
            }
        }
        if (settings.getAsBoolean("shield.audit.logfile.prefix.node_name", true)) {
            String name = settings.get("name");
            if (name != null) {
                builder.append("[").append(name).append("] ");
            }
        }
        return builder.toString();
    }
}
