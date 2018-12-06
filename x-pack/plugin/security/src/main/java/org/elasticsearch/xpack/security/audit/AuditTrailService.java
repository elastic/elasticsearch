/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

public class AuditTrailService implements AuditTrail {

    private final XPackLicenseState licenseState;
    private final List<AuditTrail> auditTrails;

    @Override
    public String name() {
        return "service";
    }

    public AuditTrailService(List<AuditTrail> auditTrails, XPackLicenseState licenseState) {
        this.auditTrails = Collections.unmodifiableList(auditTrails);
        this.licenseState = licenseState;
    }

    /** Returns the audit trail implementations that this service delegates to. */
    public List<AuditTrail> getAuditTrails() {
        return auditTrails;
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(requestId, realm, user, request);
            }
        }
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(requestId, realm, user, action, message);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(requestId, action, message);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(requestId, request);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, request);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, realm, token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, token, request);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(requestId, realm, token, request);
            }
        }
    }

    @Override
    public void accessGranted(String requestId, Authentication authentication, String action, TransportMessage msg, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessGranted(requestId, authentication, action, msg, roleNames);
            }
        }
    }

    @Override
    public void accessDenied(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessDenied(requestId, authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, request);
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, action, message);
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, User user, String action, TransportMessage request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(requestId, user, action, request);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionGranted(inetAddress, profile, rule);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionDenied(inetAddress, profile, rule);
            }
        }
    }

    @Override
    public void runAsGranted(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsGranted(requestId, authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(requestId, authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, RestRequest request, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(requestId, authentication, request, roleNames);
            }
        }
    }
}
