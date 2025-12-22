/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;

class TrialLicenseStateDiskBBQPlugin extends DiskBBQPlugin {
    // A license state constructed like this is considered a trial license
    XPackLicenseState licenseState = new XPackLicenseState(() -> 0L);

    TrialLicenseStateDiskBBQPlugin(Settings settings) {
        super(settings);
    }

    @Override
    protected XPackLicenseState getLicenseState() {
        return licenseState;
    }
}
