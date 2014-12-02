/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.ProfileIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.net.InetAddress;
import java.util.Set;

/**
 *
 */
public class AuditTrailService extends AbstractComponent implements AuditTrail {

    final AuditTrail[] auditTrails;

    @Override
    public String name() {
        return "service";
    }

    @Inject
    public AuditTrailService(Settings settings, Set<AuditTrail> auditTrails) {
        super(settings);
        this.auditTrails = auditTrails.toArray(new AuditTrail[auditTrails.size()]);
    }

    @Override
    public void anonymousAccess(String action, TransportMessage<?> message) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.anonymousAccess(action, message);
        }
    }

    @Override
    public void anonymousAccess(RestRequest request) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.anonymousAccess(request);
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.authenticationFailed(token, action, message);
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.authenticationFailed(realm, token, action, message);
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.authenticationFailed(token, request);
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.authenticationFailed(realm, token, request);
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.accessGranted(user, action, message);
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.accessDenied(user, action, message);
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.tamperedRequest(user, action, request);
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, ProfileIpFilterRule rule) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.connectionGranted(inetAddress, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, ProfileIpFilterRule rule) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.connectionDenied(inetAddress, rule);
        }
    }
}
