/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

public class BuildParameterExtension {

    private final Provider<Boolean> inFipsJvm;

    public BuildParameterExtension(ProviderFactory providers) {
        this.inFipsJvm = providers.systemProperty("tests.fips.enabled").map(BuildParameterExtension::parseBoolean);
    }

    private static boolean parseBoolean(String s) {
        if (s == null) {
            return false;
        }
        return Boolean.parseBoolean(s);
    }

    public boolean getInFipsJvm() {
        return inFipsJvm.getOrElse(false);
    }

    public void withFipsEnabledOnly(Task task) {
         task.onlyIf("FIPS mode disabled", task1 -> getInFipsJvm() == false);
    }
}
