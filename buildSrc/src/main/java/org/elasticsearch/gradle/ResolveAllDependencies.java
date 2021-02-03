/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.deprecation.DeprecatableConfiguration;

import java.util.Collection;

import static org.elasticsearch.gradle.DistributionDownloadPlugin.DISTRO_EXTRACTED_CONFIG_PREFIX;

public class ResolveAllDependencies extends DefaultTask {

    Collection<Configuration> configs;

    @TaskAction
    void resolveAll() {
        configs.stream().filter(it -> canBeResolved(it)).forEach(it -> it.resolve());
    }

    static boolean canBeResolved(Configuration configuration) {
        if (configuration.isCanBeResolved() == false) {
            return false;
        }
        if (configuration instanceof org.gradle.internal.deprecation.DeprecatableConfiguration) {
            var deprecatableConfiguration = (DeprecatableConfiguration) configuration;
            if (deprecatableConfiguration.canSafelyBeResolved() == false) {
                return false;
            }
        }
        return configuration.getName().startsWith(DISTRO_EXTRACTED_CONFIG_PREFIX) == false;
    }
}
