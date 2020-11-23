/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

public class OperatorPrivileges {

    public static final Setting<Boolean> OPERATOR_PRIVILEGES_ENABLED =
        Setting.boolSetting("xpack.security.operator_privileges.enabled", false, Setting.Property.NodeScope);

    private final OperatorUserDescriptor operatorUserDescriptor;
    private final CompositeOperatorOnly compositeOperatorOnly;
    private final XPackLicenseState licenseState;
    private final boolean enabled;

    public OperatorPrivileges(Settings settings, XPackLicenseState licenseState,
                              OperatorUserDescriptor operatorUserDescriptor, CompositeOperatorOnly compositeOperatorOnly) {
        this.operatorUserDescriptor = operatorUserDescriptor;
        this.compositeOperatorOnly = compositeOperatorOnly;
        this.licenseState = licenseState;
        this.enabled = OPERATOR_PRIVILEGES_ENABLED.get(settings);
    }

    public void maybeMarkOperatorUser(Authentication authentication, ThreadContext threadContext) {
        if (shouldProcess() && operatorUserDescriptor.isOperatorUser(authentication)) {
            threadContext.putHeader(
                AuthenticationField.PRIVILEGE_CATEGORY_KEY,
                AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
        }
    }

    public ElasticsearchSecurityException check(String action, TransportRequest request, ThreadContext threadContext) {
        if (false == shouldProcess()) {
            return null;
        }
        final OperatorOnly.Result operatorOnlyCheckResult = compositeOperatorOnly.check(action, request);
        if (operatorOnlyCheckResult.getStatus() == OperatorOnly.Status.YES) {
            if (false == AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
                threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY))) {
                return new ElasticsearchSecurityException(
                    "Operator privileges are required for " + operatorOnlyCheckResult.getMessage());
            }
        }
        return null;
    }

    private boolean shouldProcess() {
        return enabled && licenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES);
    }
}
