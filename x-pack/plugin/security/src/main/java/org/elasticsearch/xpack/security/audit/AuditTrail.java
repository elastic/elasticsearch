/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetSocketAddress;

public interface AuditTrail {

    String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";

    String name();

    void authenticationSuccess(RestRequest request);

    void authenticationSuccess(String requestId, Authentication authentication, String action, TransportRequest transportRequest);

    void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest);

    void anonymousAccessDenied(String requestId, HttpPreRequest request);

    void authenticationFailed(String requestId, HttpPreRequest request);

    void authenticationFailed(String requestId, String action, TransportRequest transportRequest);

    void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest);

    void authenticationFailed(String requestId, AuthenticationToken token, HttpPreRequest request);

    void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action, TransportRequest transportRequest);

    void authenticationFailed(String requestId, String realm, AuthenticationToken token, HttpPreRequest request);

    void accessGranted(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    );

    void accessDenied(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    );

    void tamperedRequest(String requestId, HttpPreRequest request);

    void tamperedRequest(String requestId, String action, TransportRequest transportRequest);

    void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest);

    /**
     * The {@link #connectionGranted(InetSocketAddress, String, SecurityIpFilterRule)} and
     * {@link #connectionDenied(InetSocketAddress, String, SecurityIpFilterRule)} methods do not have a requestId because they related to a
     * potentially long-lived TCP connection, not a single request. For both Transport and Rest connections, a single connection
     * granted/denied event is generated even if that connection is used for multiple Elasticsearch actions (potentially as different users)
     */
    void connectionGranted(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule);

    void connectionDenied(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule);

    void runAsGranted(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    );

    void runAsDenied(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    );

    void runAsDenied(String requestId, Authentication authentication, HttpPreRequest request, AuthorizationInfo authorizationInfo);

    /**
     * This is a "workaround" method to log index "access_granted" and "access_denied" events for actions not tied to a
     * {@code TransportMessage}, or when the connection is not 1:1, i.e. several audit events for an action associated with the same
     * message. It is currently only used to audit the resolved index (alias) name for each {@code BulkItemRequest} comprised by a
     * {@code BulkShardRequest}. We should strive to not use this and TODO refactor it out!
     */
    void explicitIndexAccessEvent(
        String requestId,
        AuditLevel eventType,
        Authentication authentication,
        String action,
        String[] indices,
        String requestName,
        InetSocketAddress remoteAddress,
        AuthorizationInfo authorizationInfo
    );

    // this is the only audit method that is called *after* the action executed, when the response is available
    // it is however *only called for coordinating actions*, which are the actions that a client invokes as opposed to
    // the actions that a node invokes in order to service a client request
    void coordinatingActionResponse(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        TransportResponse transportResponse
    );
}
