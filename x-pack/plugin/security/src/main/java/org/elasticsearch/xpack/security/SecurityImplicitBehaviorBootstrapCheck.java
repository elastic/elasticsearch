/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.XPackSettings;

public class SecurityImplicitBehaviorBootstrapCheck implements BootstrapCheck {

    private final NodeMetadata nodeMetadata;
    private final LicenseService licenseService;

    public SecurityImplicitBehaviorBootstrapCheck(NodeMetadata nodeMetadata, LicenseService licenseService) {
        this.nodeMetadata = nodeMetadata;
        this.licenseService = licenseService;
    }

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (nodeMetadata == null) {
            return BootstrapCheckResult.success();
        }
        if (licenseService instanceof ClusterStateLicenseService clusterStateLicenseService) {
            final License license = clusterStateLicenseService.getLicense(context.metadata());
            final Version lastKnownVersion = nodeMetadata.previousNodeVersion();
            // pre v7.2.0 nodes have Version.EMPTY and its id is 0, so Version#before handles this successfully
            if (lastKnownVersion.before(Version.V_8_0_0)
                && XPackSettings.SECURITY_ENABLED.exists(context.settings()) == false
                && (license.operationMode() == License.OperationMode.BASIC || license.operationMode() == License.OperationMode.TRIAL)) {
                return BootstrapCheckResult.failure(
                    "The default value for ["
                        + XPackSettings.SECURITY_ENABLED.getKey()
                        + "] has changed in the current version. "
                        + " Security features were implicitly disabled for this node but they would now be enabled, possibly"
                        + " preventing access to the node. "
                        + "See https://www.elastic.co/guide/en/elasticsearch/reference/"
                        + Version.CURRENT.major
                        + "."
                        + Version.CURRENT.minor
                        + "/security-minimal-setup.html to configure security, or explicitly disable security by "
                        + "setting [xpack.security.enabled] to \"false\" in elasticsearch.yml before restarting the node."
                );
            }
        }
        return BootstrapCheckResult.success();
    }

    public boolean alwaysEnforce() {
        return true;
    }
}
