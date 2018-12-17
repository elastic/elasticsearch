/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;

public interface AuditTrail {

    String name();

    void authenticationSuccess(String requestId, String realm, User user, RestRequest request);

    void authenticationSuccess(String requestId, String realm, User user, String action, TransportMessage message);

    void anonymousAccessDenied(String requestId, String action, TransportMessage message);

    void anonymousAccessDenied(String requestId, RestRequest request);

    void authenticationFailed(String requestId, RestRequest request);

    void authenticationFailed(String requestId, String action, TransportMessage message);

    void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportMessage message);

    void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request);

    void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action, TransportMessage message);

    void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request);

    void accessGranted(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames);

    void accessDenied(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames);

    void tamperedRequest(String requestId, RestRequest request);

    void tamperedRequest(String requestId, String action, TransportMessage message);

    void tamperedRequest(String requestId, User user, String action, TransportMessage request);

    /**
     * The {@link #connectionGranted(InetAddress, String, SecurityIpFilterRule)} and
     * {@link #connectionDenied(InetAddress, String, SecurityIpFilterRule)} methods do not have a requestId because they related to a
     * potentially long-lived TCP connection, not a single request. For both Transport and Rest connections, a single connection
     * granted/denied event is generated even if that connection is used for multiple Elasticsearch actions (potentially as different users)
     */
    void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule);

    void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule);

    void runAsGranted(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames);

    void runAsDenied(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames);

    void runAsDenied(String requestId, Authentication authentication, RestRequest request, String[] roleNames);

}
