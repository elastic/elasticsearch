/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.transport.filter.ProfileIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.net.InetAddress;

/**
 *
 */
public class LoggingAuditTrail implements AuditTrail {

    public static final String NAME = "logfile";

    private static final Predicate<String> SYSTEM_ACTION_MATCHER = Privilege.SYSTEM.predicate();

    private final ESLogger logger;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public LoggingAuditTrail(Settings settings) {
        this(Loggers.getLogger(LoggingAuditTrail.class, settings));
    }

    LoggingAuditTrail(ESLogger logger) {
        this.logger = logger;
    }

    @Override
    public void anonymousAccess(String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("ANONYMOUS_ACCESS\thost=[{}], action=[{}], indices=[{}], request=[{}]", message.remoteAddress(), action, indices, message);
            } else {
                logger.warn("ANONYMOUS_ACCESS\thost=[{}], action=[{}], indices=[{}]", message.remoteAddress(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("ANONYMOUS_ACCESS\thost=[{}], action=[{}], request=[{}]", message.remoteAddress(), action, message);
            } else {
                logger.warn("ANONYMOUS_ACCESS\thost=[{}], action=[{}]", message.remoteAddress(), action);
            }
        }
    }

    @Override
    public void anonymousAccess(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("ANONYMOUS_ACCESS\thost=[{}], URI=[{}], request=[{}]", request.getRemoteAddress(), request.uri(), request);
        } else {
            logger.warn("ANONYMOUS_ACCESS\thost=[{}], URI=[{}]", request.getRemoteAddress(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", message.remoteAddress(), token.principal(), action, indices, message);
            } else {
                logger.error("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", message.remoteAddress(), token.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}], request=[{}]", message.remoteAddress(), token.principal(), action, message);
            } else {
                logger.error("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], action=[{}]", message.remoteAddress(), token.principal(), action);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], URI=[{}], request=[{}]", request.getRemoteAddress(), token.principal(), request.uri(), request);
        } else {
            logger.error("AUTHENTICATION_FAILED\thost=[{}], principal=[{}], URI=[{}]", request.getRemoteAddress(), token.principal(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (logger.isTraceEnabled()) {
            String indices = indices(message);
            if (indices != null) {
                logger.trace("AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", realm, message.remoteAddress(), token.principal(), action, indices, message);
            } else {
                logger.trace("AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], action=[{}], request=[{}]", realm, message.remoteAddress(), token.principal(), action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("AUTHENTICATION_FAILED[{}]\thost=[{}], principal=[{}], URI=[{}], request=[{}]", realm, request.getRemoteAddress(), token.principal(), request.uri(), request);
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        String indices = indices(message);

        // special treatment for system actions - only log on trace
        if (SYSTEM_ACTION_MATCHER.apply(action)) {
            if (logger.isTraceEnabled()) {
                if (indices != null) {
                    logger.trace("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, indices, message);
                } else {
                    logger.trace("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, message);
                }
            }
            return;
        }

        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, indices, message);
            } else {
                logger.info("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", message.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, message);
            } else {
                logger.info("ACCESS_GRANTED\thost=[{}], principal=[{}], action=[{}]", message.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        String indices = indices(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, indices, message);
            } else {
                logger.error("ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], indices=[{}]", message.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}], request=[{}]", message.remoteAddress(), user.principal(), action, message);
            } else {
                logger.error("ACCESS_DENIED\thost=[{}], principal=[{}], action=[{}]", message.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        String indices = indices(request);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], indices=[{}], request=[{}]", request.remoteAddress(), user.principal(), action, indices, request);
            } else {
                logger.error("TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], indices=[{}]", request.remoteAddress(), user.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}], request=[{}]", request.remoteAddress(), user.principal(), action, request);
            } else {
                logger.error("TAMPERED REQUEST\thost=[{}], principal=[{}], action=[{}]", request.remoteAddress(), user.principal(), action);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, ProfileIpFilterRule rule) {
        if (logger.isTraceEnabled()) {
            logger.trace("CONNECTION_GRANTED\thost=[{}], rule=[{}]", inetAddress.getHostAddress(), rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, ProfileIpFilterRule rule) {
        if (logger.isDebugEnabled()) {
            logger.debug("CONNECTION_DENIED\thost=[{}], rule=[{}]", inetAddress.getHostAddress(), rule);
        } else {
            logger.error("CONNECTION_DENIED\thost=[{}]", inetAddress.getHostAddress());
        }
    }

    private static String indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return Strings.arrayToCommaDelimitedString(((IndicesRequest) message).indices());
        }
        return null;
    }
}
