/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.plugins.LicenseCheckerPlugin;
import org.elasticsearch.plugins.Plugin;

public class MockLicenseCheckerPlugin extends Plugin implements LicenseCheckerPlugin {
    public MockLicenseCheckerPlugin() {
    }

    @Override
    public boolean isRecoveryFromSnapshotAllowed() {
        return false;
    }
}
