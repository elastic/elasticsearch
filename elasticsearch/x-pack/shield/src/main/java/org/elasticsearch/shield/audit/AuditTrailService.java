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
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;

import java.net.InetAddress;
import java.util.Set;

/**
 *
 */
public class AuditTrailService extends AbstractComponent implements AuditTrail {

    private final SecurityLicenseState securityLicenseState;
    final AuditTrail[] auditTrails;

    @Override
    public String name() {
        return "service";
    }

    @Inject
    public AuditTrailService(Settings settings, Set<AuditTrail> auditTrails, SecurityLicenseState licenseState) {
        super(settings);
        this.auditTrails = auditTrails.toArray(new AuditTrail[auditTrails.size()]);
        this.securityLicenseState = licenseState;
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(action, message);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(request);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(request);
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(realm, token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(token, request);
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(realm, token, request);
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessGranted(user, action, message);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessDenied(user, action, message);
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        for (AuditTrail auditTrail : auditTrails) {
            auditTrail.tamperedRequest(request);
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(action, message);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(user, action, request);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionGranted(inetAddress, profile, rule);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.connectionDenied(inetAddress, profile, rule);
            }
        }
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsGranted(user, action, message);
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (securityLicenseState.auditingEnabled()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(user, action, message);
            }
        }
    }
}
