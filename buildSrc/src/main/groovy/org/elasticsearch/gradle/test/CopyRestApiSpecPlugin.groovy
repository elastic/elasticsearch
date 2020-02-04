package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy

/**
 * Copies the core REST specs to the local resource directory for the REST YAML tests.
 */
class CopyRestApiSpecPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.with {
            configurations.create('restSpec')
            dependencies.add(
                'restSpec',
                BuildParams.internal ? project.project(':rest-api-spec') :
                    "org.elasticsearch:rest-api-spec:${VersionProperties.elasticsearch}"
            )

            tasks.create("copyCoreRestSpecLocal", Copy) {
                dependsOn project.configurations.restSpec
                into(project.sourceSets.test.output.resourcesDir)
                from({ project.zipTree(project.configurations.restSpec.singleFile) }) {
                    includeEmptyDirs = false
                    include 'rest-api-spec/api/**'
                }
            }

            tasks.withType(RestTestRunnerTask).each { Task t ->
                t.dependsOn('copyCoreRestSpecLocal')
            }
        }
    }
}
