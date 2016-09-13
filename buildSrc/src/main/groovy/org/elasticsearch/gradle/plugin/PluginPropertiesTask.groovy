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

import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Task
import org.gradle.api.tasks.Copy

/**
 * Creates a plugin descriptor.
 */
class PluginPropertiesTask extends Copy {

    PluginPropertiesExtension extension
    File generatedResourcesDir = new File(project.buildDir, 'generated-resources')

    PluginPropertiesTask() {
        File templateFile = new File(project.buildDir, 'templates/plugin-descriptor.properties')
        Task copyPluginPropertiesTemplate = project.tasks.create('copyPluginPropertiesTemplate') {
            doLast {
                InputStream resourceTemplate = PluginPropertiesTask.getResourceAsStream('/plugin-descriptor.properties')
                templateFile.parentFile.mkdirs()
                templateFile.setText(resourceTemplate.getText('UTF-8'), 'UTF-8')
            }
        }
        dependsOn(copyPluginPropertiesTemplate)
        extension = project.extensions.create('esplugin', PluginPropertiesExtension, project)
        project.clean.delete(generatedResourcesDir)
        project.afterEvaluate {
            // check require properties are set
            if (extension.name == null) {
                throw new InvalidUserDataException('name is a required setting for esplugin')
            }
            if (extension.description == null) {
                throw new InvalidUserDataException('description is a required setting for esplugin')
            }
            if (extension.classname == null) {
                throw new InvalidUserDataException('classname is a required setting for esplugin')
            }
            // configure property substitution
            from(templateFile)
            into(generatedResourcesDir)
            Map<String, String> properties = generateSubstitutions()
            expand(properties)
            inputs.properties(properties)
        }
    }

    Map<String, String> generateSubstitutions() {
        def stringSnap = { version ->
            if (version.endsWith("-SNAPSHOT")) {
               return version.substring(0, version.length() - 9)
            }
            return version
        }
        return [
            'name': extension.name,
            'description': extension.description,
            'version': stringSnap(extension.version),
            'elasticsearchVersion': stringSnap(VersionProperties.elasticsearch),
            'javaVersion': project.targetCompatibility as String,
            'classname': extension.classname
        ]
    }
}
