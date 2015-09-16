package org.elasticsearch.gradle

import org.gradle.api.InvalidUserDataException
import org.gradle.api.internal.AbstractTask
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.OutputFile

/**
 * Creates a plugin descriptor.
 *
 * TODO: copy the example properties file to plugin documentation
 */
class PluginPropertiesTask extends AbstractTask {

    PluginPropertiesExtension extension

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
                dependsOn(project.tasks.getByName('classes')) // so we can check for the classname
            }
        }
    }

    @OutputFile
    File propertiesFile = new File(project.buildDir, "plugin" + File.separator + "plugin-descriptor.properties")

    @TaskAction
    void buildProperties() {
        Properties props = new Properties()
        props.setProperty('name', extension.name)
        props.setProperty('version', extension.version)
        props.setProperty('description', extension.description)
        props.setProperty('jvm', extension.jvm as String)
        if (extension.jvm) {
            File classesDir = project.sourceSets.main.output.classesDir
            File classFile = new File(classesDir, extension.classname.replace('.', File.separator) + '.class')
            if (classFile.exists() == false) {
                throw new InvalidUserDataException('classname ' + extension.classname + ' does not exist')
            }
            props.setProperty('classname', extension.classname)
            props.setProperty('isolated', extension.isolated as String)
            if (extension.isolated == false) {
                logger.warn('Disabling isolation is deprecated and will be removed in the future')
            }
            props.setProperty('java.version', project.targetCompatibility as String)
        }
        props.setProperty('site', extension.site as String)
        props.setProperty('elasticsearch.version', ElasticsearchProperties.version)
        props.store(propertiesFile.newWriter(), null)
    }
}
