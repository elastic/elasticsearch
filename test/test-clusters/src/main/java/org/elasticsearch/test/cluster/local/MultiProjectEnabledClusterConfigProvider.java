/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

public class MultiProjectEnabledClusterConfigProvider implements LocalClusterConfigProvider {

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        if (isMultiProjectEnabled()) {
            builder.setting("test.multi_project.enabled", "true").module("test-multi-project");
        }
    }

    private static boolean isMultiProjectEnabled() {
        // TODO: we need to use `tests` instead of `test` here to make gradle passes the system property,
        // but we need `test` in the setting.
        return Boolean.getBoolean("tests.multi_project.enabled");
    }
}
