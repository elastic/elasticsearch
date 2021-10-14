/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.license.checker;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.LicenseCheckerPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

public class XPackLicenseCheckerPlugin extends Plugin implements LicenseCheckerPlugin {
    public static final LicensedFeature.Momentary SNAPSHOT_BASED_RECOVERIES_FEATURE = LicensedFeature.momentary(
        null,
        "snapshot-based-recoveries",
        License.OperationMode.ENTERPRISE
    );

    public XPackLicenseCheckerPlugin() {}

    @Override
    public boolean isRecoveryFromSnapshotAllowed() {
        return SNAPSHOT_BASED_RECOVERIES_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }
}
