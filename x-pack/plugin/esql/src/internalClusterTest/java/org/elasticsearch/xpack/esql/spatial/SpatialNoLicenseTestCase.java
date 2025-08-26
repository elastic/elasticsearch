/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

/**
 * Utility class to provide a license state that does not allow spatial features.
 * This is used in tests to ensure that spatial functions behave correctly when no valid license is present.
 */
public abstract class SpatialNoLicenseTestCase extends ESIntegTestCase {

    private static XPackLicenseState getLicenseState() {
        License.OperationMode operationMode;
        boolean active;
        if (randomBoolean()) {
            // Randomly chosen licenses that are not Platinum, Enterprise, or Trial
            operationMode = randomFrom(
                License.OperationMode.GOLD,
                License.OperationMode.BASIC,
                License.OperationMode.MISSING,
                License.OperationMode.STANDARD
            );
            active = true;
        } else {
            // Randomly chosen licenses that are Platinum, Enterprise, or Trial but marked as expired
            operationMode = randomFrom(License.OperationMode.PLATINUM, License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL);
            active = false;  // expired
        }

        return new XPackLicenseState(
            () -> System.currentTimeMillis(),
            new XPackLicenseStatus(operationMode, active, "Test license expired")
        );
    }

    /**
     * Test plugin that provides a license state that does not allow spatial features.
     * This is used to test the behavior of spatial functions when no valid license is present.
     */
    public static class TestEsqlPlugin extends EsqlPlugin {
        protected XPackLicenseState getLicenseState() {
            return SpatialNoLicenseTestCase.getLicenseState();
        }
    }

    /**
     * Test plugin that provides a license state that does not allow spatial features.
     * This is used to test the behavior of spatial functions when no valid license is present.
     */
    public static class TestSpatialPlugin extends SpatialPlugin {
        protected XPackLicenseState getLicenseState() {
            return SpatialNoLicenseTestCase.getLicenseState();
        }
    }
}
