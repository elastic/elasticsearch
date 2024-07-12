/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.InternalTestCluster;

public class LicensingHelper {
    public static void enableLicensing(InternalTestCluster cluster, License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : cluster.getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        }
    }
}
