/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.transport.TransportMessage;

/**
 *
 */
public interface AuditTrail {

    public static final AuditTrail NOOP = new AuditTrail() {
        @Override
        public void anonymousAccess(String action, TransportMessage<?> message) {
        }

        @Override
        public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        }

        @Override
        public void accessGranted(User user, String action, TransportMessage<?> message) {
        }

        @Override
        public void accessDenied(User user, String action, TransportMessage<?> message) {
        }
    };

    void anonymousAccess(String action, TransportMessage<?> message);

    void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message);

    void accessGranted(User user, String action, TransportMessage<?> message);

    void accessDenied(User user, String action, TransportMessage<?> message);

}
