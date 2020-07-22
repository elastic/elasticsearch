/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.precommit


import org.gradle.api.Project

/**
 * Validation tasks which should be run before committing. These run before tests.
 */
class PrecommitTasks {

    /** Adds a precommit task, which depends on non-test verification tasks. */

    public static void create(Project project, boolean includeDependencyLicenses) {

        project.pluginManager.apply(CheckstylePrecommitPlugin)
        project.pluginManager.apply(ForbiddenApisPrecommitPlugin)
        project.pluginManager.apply(JarHellPrecommitPlugin)
        project.pluginManager.apply(ForbiddenPatternsPrecommitPlugin)
        project.pluginManager.apply(LicenseHeadersPrecommitPlugin)
        project.pluginManager.apply(FilePermissionsPrecommitPlugin)
        project.pluginManager.apply(ThirdPartyAuditPrecommitPlugin)
        project.pluginManager.apply(TestingConventionsPrecommitPlugin)

        // tasks with just tests don't need dependency licenses, so this flag makes adding
        // the task optional
        if (includeDependencyLicenses) {
            project.pluginManager.apply(DependencyLicensesPrecommitPlugin)
        }
        if (project.path != ':build-tools') {
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
            project.pluginManager.apply(LoggerUsagePrecommitPlugin)
        }
    }
}
