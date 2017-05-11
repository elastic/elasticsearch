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
package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy

/**
 * The rest-api-spec tests are loaded from the classpath. However, they
 * currently must be available on the local filesystem. This class encapsulates
 * setting up tasks to copy the rest spec api to test resources.
 */
public class RestSpecHack {
    /**
     * Sets dependencies needed to copy the rest spec.
     * @param project The project to add rest spec dependency to
     */
    public static void configureDependencies(Project project) {
        project.configurations {
            restSpec
        }
        project.dependencies {
            restSpec "org.elasticsearch:rest-api-spec:${VersionProperties.elasticsearch}"
        }
    }

    /**
     * Creates a task (if necessary) to copy the rest spec files.
     *
     * @param project The project to add the copy task to
     * @param includePackagedTests true if the packaged tests should be copied, false otherwise
     */
    public static Task configureTask(Project project, boolean includePackagedTests) {
        Task copyRestSpec = project.tasks.findByName('copyRestSpec')
        if (copyRestSpec != null) {
            return copyRestSpec
        }
        Map copyRestSpecProps = [
                name     : 'copyRestSpec',
                type     : Copy,
                dependsOn: [project.configurations.restSpec, 'processTestResources']
        ]
        copyRestSpec = project.tasks.create(copyRestSpecProps) {
            from { project.zipTree(project.configurations.restSpec.singleFile) }
            include 'rest-api-spec/api/**'
            if (includePackagedTests) {
                include 'rest-api-spec/test/**'
            }
            into project.sourceSets.test.output.resourcesDir
        }
        project.idea {
            module {
                if (scopes.TEST != null) {
                    scopes.TEST.plus.add(project.configurations.restSpec)
                }
            }
        }
        return copyRestSpec
    }
}
