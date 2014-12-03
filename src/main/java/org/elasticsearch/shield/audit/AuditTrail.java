/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.net.InetAddress;

/**
 *
 */
public interface AuditTrail {

    static final AuditTrail NOOP = new AuditTrail() {

        static final String NAME = "noop";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public void anonymousAccess(String action, TransportMessage<?> message) {
        }

        @Override
        public void anonymousAccess(RestRequest request) {
        }

        @Override
        public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        }

        @Override
        public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        }

        @Override
        public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        }

        @Override
        public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        }

        @Override
        public void accessGranted(User user, String action, TransportMessage<?> message) {
        }

        @Override
        public void accessDenied(User user, String action, TransportMessage<?> message) {
        }

        @Override
        public void tamperedRequest(User user, String action, TransportRequest request) {
        }

        @Override
        public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        }

        @Override
        public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        }
    };

    String name();

    void anonymousAccess(String action, TransportMessage<?> message);

    void anonymousAccess(RestRequest request);

    void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message);

    void authenticationFailed(AuthenticationToken token, RestRequest request);

    void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message);

    void authenticationFailed(String realm, AuthenticationToken token, RestRequest request);

    void accessGranted(User user, String action, TransportMessage<?> message);

    void accessDenied(User user, String action, TransportMessage<?> message);

    void tamperedRequest(User user, String action, TransportRequest request);

    void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule);

    void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule);
}
