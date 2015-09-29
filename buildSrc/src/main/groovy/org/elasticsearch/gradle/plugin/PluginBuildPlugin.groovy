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
        // this afterEvaluate must happen before the afterEvaluate added by integTest configure,
        // so that the file name resolution for installing the plugin will be setup
        project.afterEvaluate {
            project.jar.configure {
                baseName project.pluginProperties.extension.name
            }
            project.bundlePlugin.configure {
                baseName project.pluginProperties.extension.name
            }
            project.integTest.configure {
                dependsOn project.bundlePlugin
                cluster {
                    plugin 'installPlugin', project.bundlePlugin.outputs.files.singleFile
                }
            }
        }
        Task bundle = configureBundleTask(project)
        RestIntegTestTask.configure(project)
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
        return bundle
    }
}
