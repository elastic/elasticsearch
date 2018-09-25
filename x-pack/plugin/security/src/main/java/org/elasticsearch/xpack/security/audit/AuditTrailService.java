/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
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

public class AuditTrailService extends AbstractComponent implements AuditTrail {

    private final XPackLicenseState licenseState;
    private final List<AuditTrail> auditTrails;

    @Override
    public String name() {
        return "service";
    }

    public AuditTrailService(Settings settings, List<AuditTrail> auditTrails, XPackLicenseState licenseState) {
        super(settings);
        this.auditTrails = Collections.unmodifiableList(auditTrails);
        this.licenseState = licenseState;
    }

    /** Returns the audit trail implementations that this service delegates to. */
    public List<AuditTrail> getAuditTrails() {
        return auditTrails;
    }

    @Override
    public void authenticationSuccess(String realm, User user, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(realm, user, request);
            }
        }
    }

    @Override
    public void authenticationSuccess(String realm, User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationSuccess(realm, user, action, message);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(action, message);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.anonymousAccessDenied(request);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(request);
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(realm, token, action, message);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(token, request);
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.authenticationFailed(realm, token, request);
            }
        }
    }

    @Override
    public void accessGranted(Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessGranted(authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void accessDenied(Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessDenied(authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(request);
            }
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(action, message);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.tamperedRequest(user, action, request);
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
    public void runAsGranted(Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsGranted(authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void runAsDenied(Authentication authentication, String action, TransportMessage message, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(authentication, action, message, roleNames);
            }
        }
    }

    @Override
    public void runAsDenied(Authentication authentication, RestRequest request, String[] roleNames) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(authentication, request, roleNames);
            }
        }
    }
}
