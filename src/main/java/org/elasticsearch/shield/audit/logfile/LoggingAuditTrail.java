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
import org.elasticsearch.transport.TransportMessage;

/**
 *
 */
public class LoggingAuditTrail extends AbstractComponent implements AuditTrail {

    @Inject
    public LoggingAuditTrail(Settings settings) {
        super(settings);
    }

    @Override
    public void anonymousAccess(String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.info("ANONYMOUS_ACCESS\thost=[{}], action=[{}], request=[{}]", message.remoteAddress(), action, message);
        } else {
            logger.info("ANONYMOUS_ACCESS\thost=[{}], action=[{}]", message.remoteAddress(), action);
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.info("AUTHENTICATION_FAILED\thost=[{}], realm=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), realm, action, token.principal(), message);
        } else {
            logger.info("AUTHENTICATION_FAILED\thost=[{}], realm=[{}], action=[{}], principal=[{}]", message.remoteAddress(), realm, action, token.principal());
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.info("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), action, user.principal(), message);
        } else {
            logger.info("ACCESS_GRANTED\thost=[{}], action=[{}], principal=[{}]", message.remoteAddress(), action, user.principal());
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        if (logger.isDebugEnabled()) {
            logger.info("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}], request=[{}]", message.remoteAddress(), action, user.principal(), message);
        } else {
            logger.info("ACCESS_DENIED\thost=[{}], action=[{}], principal=[{}]", message.remoteAddress(), action, user.principal());
        }
    }

}
