package org.elasticsearch.gradle.plugin

import org.elasticsearch.gradle.ElasticsearchProperties
import org.gradle.api.DefaultTask
import org.gradle.api.InvalidUserDataException
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * Creates a plugin descriptor.
 *
 * TODO: copy the example properties file to plugin documentation
 */
class PluginPropertiesTask extends DefaultTask {

    PluginPropertiesExtension extension
    Map<String, String> properties = new HashMap<>()

    PluginPropertiesTask() {
        extension = project.extensions.create('esplugin', PluginPropertiesExtension, project)
        project.afterEvaluate {
            if (extension.description == null) {
                throw new InvalidUserDataException('description is a required setting for esplugin')
            }
            if (extension.jvm && extension.classname == null) {
                throw new InvalidUserDataException('classname is a required setting for esplugin with jvm=true')
            }
            if (extension.jvm) {
                dependsOn(project.classes) // so we can check for the classname
            }
            fillProperties()
            configure {
                inputs.properties(properties)
            }
        }
    }

    @OutputFile
    File propertiesFile = new File(project.buildDir, "plugin" + File.separator + "plugin-descriptor.properties")

    void fillProperties() {
        // TODO: need to copy the templated plugin-descriptor with a dependent task, since copy requires a file (not uri)
        properties = [
            'name': extension.name,
            'description': extension.description,
            'version': extension.version,
            'elasticsearch.version': ElasticsearchProperties.version,
            'jvm': extension.jvm as String,
            'site': extension.site as String
        ]
        if (extension.jvm) {
            properties['classname'] = extension.classname
            properties['isolated'] = extension.isolated as String
            properties['java.version'] = project.targetCompatibility as String
        }
    }

    @TaskAction
    void buildProperties() {
        if (extension.jvm) {
            File classesDir = project.sourceSets.main.output.classesDir
            File classFile = new File(classesDir, extension.classname.replace('.', File.separator) + '.class')
            if (classFile.exists() == false) {
                throw new InvalidUserDataException('classname ' + extension.classname + ' does not exist')
            }
            if (extension.isolated == false) {
                logger.warn('Disabling isolation is deprecated and will be removed in the future')
            }
        }

        Properties props = new Properties()
        for (Map.Entry<String, String> prop : properties) {
            props.put(prop.getKey(), prop.getValue())
        }
        props.store(propertiesFile.newWriter(), null)
    }
}
