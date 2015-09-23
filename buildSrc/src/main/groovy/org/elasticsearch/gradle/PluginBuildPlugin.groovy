package org.elasticsearch.gradle

import com.carrotsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.bundling.Zip

/**
 * Encapsulates adding all build logic and configuration for elasticsearch projects.
 */
class PluginBuildPlugin extends BuildPlugin {

    @Override
    void apply(Project project) {
        super.apply(project)
        // TODO: add target compatibility (java version) to elasticsearch properties and set for the project
        configureDependencies(project)
        configureRestSpecHack(project)
        Task bundle = configureBundleTask(project.tasks)
        project.integTest {
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

    @Override
    Class<? extends RandomizedTestingTask> getIntegTestClass() {
        return RestIntegTestTask
    }

    static void configureDependencies(Project project) {
        String elasticsearchVersion = ElasticsearchProperties.version
        project.configurations {
            // a separate configuration from compile so added dependencies can be distinguished
            provided
            compile.extendsFrom(provided)
            restSpec
        }
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${elasticsearchVersion}"
            //compile project.configurations.provided
            testCompile "org.elasticsearch:test-framework:${elasticsearchVersion}"
            restSpec "org.elasticsearch:rest-api-spec:${elasticsearchVersion}"
        }
    }

    static void configureRestSpecHack(Project project) {
        // HACK: rest spec tests should work as an included jar on the classpath
        Map copyRestSpecProps = [
            name: 'copyRestSpec',
            type: Copy,
            dependsOn: [project.configurations.restSpec.buildDependencies, 'processTestResources']
        ]
        Task copyRestSpec = project.tasks.create(copyRestSpecProps) {
            from project.zipTree(project.configurations.restSpec.asPath)
            include 'rest-api-spec/api/**'
            into project.sourceSets.test.output.resourcesDir
        }
        project.tasks.getByName('test').dependsOn copyRestSpec
        project.tasks.getByName('integTest').dependsOn copyRestSpec

        // HACK: rest test case should not try to load from the filesystem
        project.tasks.getByName('integTest').configure {
            sysProp 'tests.rest.load_packaged', 'false'
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
