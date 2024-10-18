/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rolemapping;

import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

public class RoleMappingRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String settingsJson = "{}";

    protected static LocalClusterConfigProvider additionalConfig() {
        return cluster -> cluster.configFile("operator/settings.json", Resource.fromString(settingsJson));
    }

}
