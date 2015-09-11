package org.elasticsearch.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Encapsulates adding all build logic and configuration for elasticsearch projects.
 */
class ESPluginBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('org.elasticsearch.build')

        String elasticsearchVersion = getElasticsearchVersion()
        configureDependencies(project, elasticsearchVersion)
    }

    String getElasticsearchVersion() {
        Properties props = new Properties()
        props.load(getClass().getResourceAsStream('/elasticsearch.properties'))
        return props.getProperty('version')
    }

    static void configureDependencies(Project project, String elasticsearchVersion) {
        project.dependencies {
            compile "org.elasticsearch:elasticsearch:${elasticsearchVersion}"
            testCompile "org.elasticsearch:test-framework:${elasticsearchVersion}"
        }
    }
}
