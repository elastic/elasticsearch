/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.security.OperatorPrivilegesFeatureSetUsage;

import java.util.Map;

import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;

public class OperatorPrivilegesFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;
    private final boolean enabled;

    @Inject
    public OperatorPrivilegesFeatureSet(Settings settings, XPackLicenseState licenseState) {
        this.licenseState = licenseState;
        this.enabled = OPERATOR_PRIVILEGES_ENABLED.get(settings);
    }

    @Override
    public String name() {
        return XPackField.OPERATOR_PRIVILEGES;
    }

    @Override
    public boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.OPERATOR_PRIVILEGES);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        listener.onResponse(
            new OperatorPrivilegesFeatureSetUsage(available(), enabled)
        );
    }
}
