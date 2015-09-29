package org.elasticsearch.gradle.plugin

import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.ElasticsearchProperties
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.gradle.api.Project
import org.gradle.api.Task
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
        Task bundle = configureBundleTask(project)
        RestIntegTestTask integTest = RestIntegTestTask.configure(project)
        project.afterEvaluate {
            integTest.configure {
                dependsOn bundle
                cluster {
                    plugin 'installPlugin', bundle.outputs.files.singleFile
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

    static Task configureBundleTask(Project project) {
        PluginPropertiesTask buildProperties = project.tasks.create(name: 'pluginProperties', type: PluginPropertiesTask)
        Task bundle = project.tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [project.jar, buildProperties])
        bundle.configure {
            from project.jar
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
        project.assemble.dependsOn(bundle)
        project.afterEvaluate {
            project.jar.configure {
                baseName buildProperties.extension.name
            }
            bundle.configure {
                baseName buildProperties.extension.name
            }
        }
        return bundle
    }
}
