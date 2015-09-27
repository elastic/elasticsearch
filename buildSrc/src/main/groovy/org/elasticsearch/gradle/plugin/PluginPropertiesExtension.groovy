package org.elasticsearch.gradle.plugin

import org.gradle.api.Project
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional

/**
 * A container for plugin properties that will be written to the plugin descriptor, for easy
 * manipulation in the gradle DSL.
 */
class PluginPropertiesExtension {

    @Input
    String name

    @Input
    String version

    @Input
    String description

    @Input
    boolean jvm = true

    @Input
    String classname

    @Input
    boolean site = false

    @Input
    boolean isolated = true

    PluginPropertiesExtension(Project project) {
        name = project.name
        version = project.version
    }
}
