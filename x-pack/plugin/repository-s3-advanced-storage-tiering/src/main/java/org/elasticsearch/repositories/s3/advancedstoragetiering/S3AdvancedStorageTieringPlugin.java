/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

@SuppressWarnings("unused")
public class S3AdvancedStorageTieringPlugin extends Plugin {
    public static final LicensedFeature.Momentary S3_ADVANCED_STORAGE_TIERING_FEATURE = LicensedFeature.momentary(
        null,
        "repository-s3-advanced-storage-tiering",
        License.OperationMode.ENTERPRISE
    );

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
