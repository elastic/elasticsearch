package org.elasticsearch.gradle.plugin

import com.carrotsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.ElasticsearchProperties
import org.elasticsearch.gradle.precommit.DependencyLicensesTask
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.elasticsearch.gradle.test.RestSpecHack
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.bundling.Zip

/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
class PluginBuildPlugin extends BuildPlugin {

    @Override
    void apply(Project project) {
        super.apply(project)
        // TODO: add target compatibility (java version) to elasticsearch properties and set for the project
        configureDependencies(project)
        Task bundle = configureBundleTask(project.tasks)
        RestIntegTestTask integTest = RestIntegTestTask.configure(project)
        integTest.configure {
            dependsOn bundle
            cluster {
                setup {
                    run name: "installPlugin", args: ['bin/plugin', 'install', "file://${bundle.outputs.files.singleFile}"]
                }
            }
        }
        configureDependencyLicenses(project)
        project.configurations.archives.artifacts.removeAll { it.archiveTask.is project.jar }
        project.configurations.runtime.artifacts.removeAll { it.archiveTask.is project.jar }
        project.artifacts {
            archives bundle
            'default' bundle
        }
    }

    static void configureDependencies(Project project) {
        String elasticsearchVersion = ElasticsearchProperties.version
        project.configurations {
            // a separate configuration from compile so added dependencies can be distinguished
            provided
            compile.extendsFrom(provided)
        }
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${elasticsearchVersion}"
            //compile project.configurations.provided
            testCompile "org.elasticsearch:test-framework:${elasticsearchVersion}"
        }
    }

    static Task configureBundleTask(TaskContainer tasks) {
        Task jar = tasks.getByName('jar')
        Task buildProperties = tasks.create(name: 'pluginProperties', type: PluginPropertiesTask)
        Task bundle = tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [jar, buildProperties])
        bundle.configure {
            from jar
            from buildProperties
            from bundle.project.configurations.runtime - bundle.project.configurations.provided
            from('src/main') {
                include 'config/**'
                include 'bin/**'
            }
            from('src/site') {
                include '_site/**'
            }
        }
        tasks.getByName('assemble').dependsOn(bundle)
        return bundle
    }

    static configureDependencyLicenses(Project project) {
        Task dependencyLicensesTask = DependencyLicensesTask.addToProject(project) {
            dependencies = project.configurations.runtime - project.configurations.provided
        }
        project.tasks.getByName('precommit').dependsOn(dependencyLicensesTask)
    }
}
