/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersPrecommitPlugin;
import org.gradle.api.Project;

/**
 * Internal precommit plugins that adds elasticsearch project specific
 * checks to the common precommit plugin.
 */
public class InternalPrecommitTasks {
    /**
     * Adds a precommit task, which depends on non-test verification tasks.
     */
    public static void create(Project project, boolean includeDependencyLicenses) {
        project.getPluginManager().apply(JarHellPrecommitPlugin.class);
        project.getPluginManager().apply(ThirdPartyAuditPrecommitPlugin.class);
        project.getPluginManager().apply(CheckstylePrecommitPlugin.class);
        project.getPluginManager().apply(ForbiddenApisPrecommitPlugin.class);
        project.getPluginManager().apply(ForbiddenPatternsPrecommitPlugin.class);
        project.getPluginManager().apply(LicenseHeadersPrecommitPlugin.class);
        project.getPluginManager().apply(FilePermissionsPrecommitPlugin.class);
        project.getPluginManager().apply(TestingConventionsPrecommitPlugin.class);

        // tasks with just tests don't need dependency licenses, so this flag makes adding
        // the task optional
        if (includeDependencyLicenses) {
            project.getPluginManager().apply(DependencyLicensesPrecommitPlugin.class);
        }

        if (project.getPath().equals(":build-tools") == false) {
            /*
             * Sadly, build-tools can't have logger-usage-check because that
             * would create a circular project dependency between build-tools
             * (which provides NamingConventionsCheck) and :test:logger-usage
             * which provides the logger usage check. Since the build tools
             * don't use the logger usage check because they don't have any
             * of Elaticsearch's loggers and :test:logger-usage actually does
             * use the NamingConventionsCheck we break the circular dependency
             * here.
             */
            project.getPluginManager().apply(LoggerUsagePrecommitPlugin.class);
        }
    }
}
