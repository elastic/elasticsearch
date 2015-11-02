/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
    File generatedResourcesDir = new File(project.projectDir, "generated-resources")

    PluginPropertiesTask() {
        extension = project.extensions.create('esplugin', PluginPropertiesExtension, project)
        project.clean {
            delete generatedResourcesDir
        }
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
    File propertiesFile = new File(generatedResourcesDir, "plugin-descriptor.properties")

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
