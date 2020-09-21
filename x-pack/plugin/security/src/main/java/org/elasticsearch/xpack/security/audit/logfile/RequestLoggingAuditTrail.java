/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;

public class RequestLoggingAuditTrail extends LoggingAuditTrail {

    public static final String NAME = "audit_transport_request";

    public interface RequestAuditor {

        String name();

        void auditTransportRequest(String requestId, String action, TransportRequest transportRequest, LogEntryBuilder logEntryBuilder);
    }

    private final RequestAuditor requestAuditor;

    public RequestLoggingAuditTrail(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                    RequestAuditor requestAuditor) {
        super(settings, clusterService, threadPool);
        this.requestAuditor = requestAuditor;
    }

    @Override
    public String name() {
        return NAME + "_" + requestAuditor.name();
    }

    @Override
    public void authenticationSuccess(String requestId, Authentication authentication, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.authenticationSuccess(requestId, authentication, request);
    }

    @Override
    public void authenticationSuccess(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        // "authentication success" events are always followed by some authorization event
        // only audit the transport request body for authorization events because "authentication success" events are not emitted for
        // child actions (i.e. authentication happens for the parent action only), and request bodies for child actions might
        // require auditing as well
        super.authenticationSuccess(requestId, authentication, action, transportRequest);
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.anonymousAccessDenied(requestId, action, transportRequest, logEntryBuilder);
    }

    @Override
    public void anonymousAccessDenied(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.anonymousAccessDenied(requestId, request);
    }

    @Override
    public void authenticationFailed(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.authenticationFailed(requestId, request);
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.authenticationFailed(requestId, action, transportRequest, logEntryBuilder);
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.authenticationFailed(requestId, token, action, transportRequest, logEntryBuilder);
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.authenticationFailed(requestId, token, request);
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action,
                                     TransportRequest transportRequest) {
        // "realm auth failed" events are emitted for every realm in the chain that is tried but which can't verify the credentials
        // for transport request auditing we rely on other subsequent events
        super.authenticationFailed(requestId, realm, token, action, transportRequest);
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.authenticationFailed(requestId, realm, token, request);
    }

    @Override
    public void accessGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                              AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.accessGranted(requestId, authentication, action, transportRequest, authorizationInfo, logEntryBuilder);
    }

    @Override
    public void accessDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                             AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.accessDenied(requestId, authentication, action, transportRequest, authorizationInfo, logEntryBuilder);
    }

    @Override
    public void tamperedRequest(String requestId, RestRequest request) {
        // this is a REST request; audit only transport request bodies
        super.tamperedRequest(requestId, request);
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.tamperedRequest(requestId, action, transportRequest, logEntryBuilder);
    }

    @Override
    public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.tamperedRequest(requestId, authentication, action, transportRequest, logEntryBuilder);
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        // connection events do not refer transport requests
        super.connectionGranted(inetAddress, profile, rule);
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        // connection events do not refer transport requests
        super.connectionDenied(inetAddress, profile, rule);
    }

    @Override
    public void runAsGranted(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                             AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // "run-as granted" events are always followed by access granted events, which are used to log the request body
        super.runAsGranted(requestId, authentication, action, transportRequest, authorizationInfo);
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, String action, TransportRequest transportRequest,
                            AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        LogEntryBuilder logEntryBuilder = super.logEntryBuilderSupplier.get();
        requestAuditor.auditTransportRequest(requestId, action, transportRequest, logEntryBuilder);
        super.runAsDenied(requestId, authentication, action, transportRequest, authorizationInfo, logEntryBuilder);
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, RestRequest request,
                            AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // this is a REST request; audit only transport request bodies
        super.runAsDenied(requestId, authentication, request, authorizationInfo);
    }

    @Override
    public void explicitIndexAccessEvent(String requestId, AuditLevel eventType, Authentication authentication, String action,
                                         String indices, String requestName, TransportAddress remoteAddress,
                                         AuthorizationEngine.AuthorizationInfo authorizationInfo) {
        // these are used for detailed auditing of bulk items, but it is sufficient to audit the request body of the entire bulk request
        super.explicitIndexAccessEvent(requestId, eventType, authentication, action, indices, requestName, remoteAddress,
                authorizationInfo);
    }
}
