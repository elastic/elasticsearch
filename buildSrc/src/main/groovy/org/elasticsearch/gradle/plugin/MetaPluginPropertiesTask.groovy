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

import org.gradle.api.InvalidUserDataException
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.OutputFile

class MetaPluginPropertiesTask extends Copy {

    MetaPluginPropertiesExtension extension

    @OutputFile
    File descriptorOutput = new File(project.buildDir, 'generated-resources/meta-plugin-descriptor.properties')

    MetaPluginPropertiesTask() {
        File templateFile = new File(project.buildDir, "templates/${descriptorOutput.name}")
        Task copyPluginPropertiesTemplate = project.tasks.create('copyPluginPropertiesTemplate') {
            doLast {
                InputStream resourceTemplate = PluginPropertiesTask.getResourceAsStream("/${descriptorOutput.name}")
                templateFile.parentFile.mkdirs()
                templateFile.setText(resourceTemplate.getText('UTF-8'), 'UTF-8')
            }
        }

        dependsOn(copyPluginPropertiesTemplate)
        extension = project.extensions.create('es_meta_plugin', MetaPluginPropertiesExtension, project)
        project.afterEvaluate {
            // check require properties are set
            if (extension.name == null) {
                throw new InvalidUserDataException('name is a required setting for es_meta_plugin')
            }
            if (extension.description == null) {
                throw new InvalidUserDataException('description is a required setting for es_meta_plugin')
            }
            // configure property substitution
            from(templateFile.parentFile).include(descriptorOutput.name)
            into(descriptorOutput.parentFile)
            Map<String, String> properties = generateSubstitutions()
            expand(properties)
            inputs.properties(properties)
        }
    }

    Map<String, String> generateSubstitutions() {
        return ['name': extension.name,
                'description': extension.description
        ]
    }
}
