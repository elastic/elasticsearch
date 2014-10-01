/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class LoggingAuditTrail implements AuditTrail {

    public static final String NAME = "logfile";

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
        if (logger.isDebugEnabled()) {
            logger.debug("ANONYMOUS_ACCESS\thost=[{}], action=[{}], request=[{}]", message.remoteAddress(), action, message);
        } else {
            logger.warn("ANONYMOUS_ACCESS\thost=[{}], action=[{}]", message.remoteAddress(), action);
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.debug("AUTHENTICATION_FAILED\thost=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), action, token.principal(), message);
        } else {
            logger.error("AUTHENTICATION_FAILED\thost=[{}], action=[{}], principal=[{}]", message.remoteAddress(), action, token.principal());
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("AUTHENTICATION_FAILED\thost=[{}], URI=[{}], principal=[{}], request=[{}]", request.getRemoteAddress(), request.uri(), token.principal(), request);
        } else {
            logger.error("AUTHENTICATION_FAILED\thost=[{}], URI=[{}], principal=[{}]", request.getRemoteAddress(), request.uri(), token.principal());
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (logger.isTraceEnabled()) {
            logger.trace("AUTHENTICATION_FAILED[{}]\thost=[{}], action=[{}], principal=[{}], request=[{}]", realm, message.remoteAddress(), action, token.principal(), message);
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("AUTHENTICATION_FAILED[{}]\thost=[{}], URI=[{}], principal=[{}], request=[{}]", realm, request.getRemoteAddress(), request.uri(), token.principal(), request);
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.debug("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), action, user.principal(), message);
        } else {
            logger.info("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}]", message.remoteAddress(), action, user.principal());
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.debug("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), action, user.principal(), message);
        } else {
            logger.error("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}]", message.remoteAddress(), action, user.principal());
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("TAMPERED REQUEST\thost=[{}], action=[{}], principal=[{}], request=[{}]", request.remoteAddress(), action, user.principal(), request);
        } else {
            logger.error("TAMPERED REQUEST\thost=[{}], action=[{}], principal=[{}]", request.remoteAddress(), action, user.principal());
        }
    }
}
