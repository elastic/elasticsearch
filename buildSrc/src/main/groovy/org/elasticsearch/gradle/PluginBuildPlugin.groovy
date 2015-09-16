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
            cluster {
                setup {
                    run name: "installPlugin", args: ['bin/plugin', 'install', "file://${bundle.outputs.files.asPath}"]
                }
            }
        }
        project.configurations.archives.artifacts.removeAll { it.archiveTask.is project.jar }
        project.configurations.runtime.artifacts.removeAll { it.archiveTask.is project.jar }
        project.artifacts {
            archives bundle
            'default' bundle
        }
    }

    @Override
    Class<? extends RandomizedTestingTask> getIntegTestClass() {
        return IntegTestTask
    }

    static void configureDependencies(Project project) {
        String elasticsearchVersion = ElasticsearchProperties.version
        project.configurations {
            // a separate configuration from compile so added dependencies can be distinguished
            provided
            restSpec
        }
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${elasticsearchVersion}"
            testCompile "org.elasticsearch:test-framework:${elasticsearchVersion}"
            restSpec "org.elasticsearch:rest-api-spec:${elasticsearchVersion}"
        }
        project.sourceSets.main.compileClasspath += [project.configurations.provided]
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
            // required by the test framework
            include 'rest-api-spec/api/info.json'
            include 'rest-api-spec/api/cluster.health.json'
            include 'rest-api-spec/api/cluster.state.json'
            include 'rest-api-spec/api/cluster.state.json'
            // used in plugin REST tests
            include 'rest-api-spec/api/index.json'
            include 'rest-api-spec/api/get.json'
            include 'rest-api-spec/api/update.json'
            include 'rest-api-spec/api/search.json'
            include 'rest-api-spec/api/indices.analyze.json'
            include 'rest-api-spec/api/indices.create.json'
            include 'rest-api-spec/api/indices.refresh.json'
            include 'rest-api-spec/api/nodes.info.json'
            include 'rest-api-spec/api/count.json'
            // used in repository plugin REST tests
            include 'rest-api-spec/api/snapshot.create_repository.json'
            include 'rest-api-spec/api/snapshot.get_repository.json'
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
}
