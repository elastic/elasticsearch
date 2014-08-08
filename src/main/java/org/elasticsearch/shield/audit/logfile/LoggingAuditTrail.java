/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class LoggingAuditTrail extends AbstractComponent implements AuditTrail {

    @Inject
    public LoggingAuditTrail(Settings settings) {
        super(settings);
    }

    @Override
    public void anonymousAccess(String action, TransportRequest request) {
        if (logger.isDebugEnabled()) {
            logger.info("ANONYMOUS_ACCESS\thost=[{}], action=[{}], request=[{}]", request.remoteAddress(), action, request);
        } else {
            logger.info("ANONYMOUS_ACCESS\thost=[{}], action=[{}]", request.remoteAddress(), action);
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportRequest request) {
        if (logger.isDebugEnabled()) {
            logger.info("AUTHENTICATION_FAILED\thost=[{}], realm=[{}], action=[{}], principal=[{}], request=[{}]", request.remoteAddress(), realm, action, token.principal(), request);
        } else {
            logger.info("AUTHENTICATION_FAILED\thost=[{}], realm=[{}], action=[{}], principal=[{}]", request.remoteAddress(), realm, action, token.principal());
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportRequest request) {
        if (logger.isDebugEnabled()) {
            logger.info("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}], request=[{}]", request.remoteAddress(), action, user.principal(), request);
        } else {
            logger.info("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}]", request.remoteAddress(), action, user.principal());
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportRequest request) {
        if (logger.isDebugEnabled()) {
            logger.info("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}], request=[{}]", request.remoteAddress(), action, user.principal(), request);
        } else {
            logger.info("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}]", request.remoteAddress(), action, user.principal());
        }
    }

}
