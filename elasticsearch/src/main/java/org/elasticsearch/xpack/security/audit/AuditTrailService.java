/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.xpack.security.user.User;

public class AuditTrailService extends AbstractComponent implements AuditTrail {

    public static final Map<String, Object> DISABLED_USAGE_STATS = Collections.singletonMap("enabled", false);

    private final XPackLicenseState licenseState;
    final List<AuditTrail> auditTrails;

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
    public void accessGranted(User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.accessGranted(user, action, message);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
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
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsGranted(user, action, message);
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(user, action, message);
            }
        }
    }

    @Override
    public void runAsDenied(User user, RestRequest request) {
        if (licenseState.isAuditingAllowed()) {
            for (AuditTrail auditTrail : auditTrails) {
                auditTrail.runAsDenied(user, request);
            }
        }
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> map = new HashMap<>(2);
        map.put("enabled", XPackSettings.AUDIT_ENABLED.get(settings));
        map.put("outputs", Security.AUDIT_OUTPUTS_SETTING.get(settings));
        return map;
    }
}
