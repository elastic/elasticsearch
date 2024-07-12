/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateS3AdvancedStorageTieringPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateS3AdvancedStorageTieringPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath);
    }

    @Override
    protected void setLicenseState(XPackLicenseState licenseState) {
        super.setLicenseState(licenseState);
        AdvancedS3StorageClassStrategyProvider.licenseStateSupplier = () -> licenseState;
    }
}
