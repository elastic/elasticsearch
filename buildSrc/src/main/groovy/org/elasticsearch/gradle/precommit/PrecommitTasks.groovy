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

import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin

/**
 * Validation tasks which should be run before committing. These run before tests.
 */
class PrecommitTasks {

    /** Adds a precommit task, which depends on non-test verification tasks. */
    public static Task create(Project project, boolean includeDependencyLicenses) {

        List<Task> precommitTasks = [
            configureForbiddenApis(project),
            project.tasks.create('forbiddenPatterns', ForbiddenPatternsTask.class),
            project.tasks.create('jarHell', JarHellTask.class)]

        // tasks with just tests don't need dependency licenses, so this flag makes adding
        // the task optional
        if (includeDependencyLicenses) {
            DependencyLicensesTask dependencyLicenses = project.tasks.create('dependencyLicenses', DependencyLicensesTask.class)
            precommitTasks.add(dependencyLicenses)
            // we also create the updateShas helper task that is associated with dependencyLicenses
            UpdateShasTask updateShas = project.tasks.create('updateShas', UpdateShasTask.class)
            updateShas.parentTask = dependencyLicenses
        }

        Map<String, Object> precommitOptions = [
            name: 'precommit',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs all non-test checks.',
            dependsOn: precommitTasks
        ]
        return project.tasks.create(precommitOptions)
    }

    private static Task configureForbiddenApis(Project project) {
        project.pluginManager.apply(ForbiddenApisPlugin.class)
        project.forbiddenApis {
            internalRuntimeForbidden = true
            failOnUnsupportedJava = false
            bundledSignatures = ['jdk-unsafe', 'jdk-deprecated']
            signaturesURLs = [getClass().getResource('/forbidden/all-signatures.txt')]
            suppressAnnotations = ['**.SuppressForbidden']
        }
        Task mainForbidden = project.tasks.findByName('forbiddenApisMain')
        if (mainForbidden != null) {
            mainForbidden.configure {
                bundledSignatures += 'jdk-system-out'
                signaturesURLs += [
                        getClass().getResource('/forbidden/core-signatures.txt'),
                        getClass().getResource('/forbidden/third-party-signatures.txt')]
            }
        }
        Task testForbidden = project.tasks.findByName('forbiddenApisTest')
        if (testForbidden != null) {
            testForbidden.configure {
                signaturesURLs += getClass().getResource('/forbidden/test-signatures.txt')
            }
        }
        Task forbiddenApis = project.tasks.findByName('forbiddenApis')
        forbiddenApis.group = "" // clear group, so this does not show up under verification tasks
        return forbiddenApis
    }
}
