/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackFeatureSet;

/**
 *
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final SecurityLicenseState licenseState;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable SecurityLicenseState licenseState) {
        this.enabled = Security.enabled(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return Security.NAME;
    }

    @Override
    public String description() {
        return "Security for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.authenticationAndAuthorizationEnabled();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }
}
