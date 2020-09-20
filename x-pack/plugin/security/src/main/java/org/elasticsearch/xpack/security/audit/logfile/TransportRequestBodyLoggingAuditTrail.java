/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;

public class TransportRequestBodyLoggingAuditTrail implements AuditTrail {

    public static final String NAME = "logfile_transport_request_body";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void authenticationSuccess(String requestId, Authentication authentication, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void authenticationSuccess(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        // "authentication success" events are always followed by some authorization event
        // only audit the transport request body for authorization events because "authentication success" events are not emitted for
        // child actions (i.e. authentication happens for the parent action only), and request bodies for child actions might
        // require auditing as well
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {
        //TODO
    }

    @Override
    public void anonymousAccessDenied(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void authenticationFailed(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {
        // TODO
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {
        // TODO
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action,
                                     TransportRequest transportRequest) {
        // "realm auth failed" events are emitted for every realm in the chain that is tried but which can't verify the credentials
        // for transport request auditing we rely on other subsequent events
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void accessGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                              AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // TODO
    }

    @Override
    public void accessDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                             AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // TODO
    }

    @Override
    public void tamperedRequest(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {
        // TODO
    }

    @Override
    public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        // TODO
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        // connection events do not refer transport requests
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        // connection events do not refer transport requests
    }

    @Override
    public void runAsGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                             AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // "run-as granted" events are always followed by access granted events, which are used to log the request body
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                            AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // TODO
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, RestRequest request,
                            AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // TODO
    }

    @Override
    public void explicitIndexAccessEvent(String requestId, AuditLevel eventType, Authentication authentication, String action,
                                         String indices, String requestName, TransportAddress remoteAddress,
                                         AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // these are used for detailed auditing of bulk items, but it is sufficient to audit the request body of the entire bulk request
    }
}
