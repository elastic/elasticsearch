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
package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates a plugin descriptor.
 */
public class PluginPropertiesTask extends Copy {

    private PluginPropertiesExtension extension;

    private File descriptorOutput = new File(getProject().getBuildDir(),
            "generated-resources/plugin-descriptor.properties");

    public PluginPropertiesTask() {
        Project project = this.getProject();

        File templateFile = new File(project.getBuildDir(), "templates/" + this.descriptorOutput.getName());

        extension = project.getExtensions().create("esplugin", PluginPropertiesExtension.class, project);
        project.afterEvaluate((unused) -> {
            from(templateFile.getParentFile()).include(this.descriptorOutput.getName());
            into(this.descriptorOutput.getParentFile());
            Map<String, String> properties = this.generateSubstitutions();
            expand(properties);
            this.getInputs().properties(properties);
        });
    }

    @TaskAction
    public void configurePluginPropertiesTask() {
        Project project = this.getProject();

        File templateFile = new File(project.getBuildDir(), "templates/" + this.descriptorOutput.getName());
        templateFile.getParentFile().mkdirs();

        Task copyPluginPropertiesTemplate = project.getTasks().create("copyPluginPropertiesTemplate")
            .doLast((unused) -> {
                from("/" + this.descriptorOutput.getName());
                into(templateFile.toPath());
            });

        dependsOn(copyPluginPropertiesTemplate);
    }

    private Map<String, String> generateSubstitutions() {
        Map<String, String> substitutions = new HashMap<>();
        substitutions.put("name", this.extension.getName());
        substitutions.put("description", this.extension.getDescription());
        substitutions.put("version", this.extension.getVersion());
        substitutions.put("elasticsearchVersion", Version.fromString(VersionProperties.getElasticsearch()).toString());
        substitutions.put("javaVersion", getProject().getConvention().getPlugin(JavaPluginConvention.class)
                .getTargetCompatibility().toString());
        substitutions.put("classname", this.extension.getClassname());
        substitutions.put("extendedPlugins", this.extension.getExtendedPlugins().stream().collect(Collectors.joining(",")));
        substitutions.put("hasNativeController", String.valueOf(this.extension.isHasNativeController()));
        substitutions.put("requiresKeystore", String.valueOf(this.extension.isRequiresKeystore()));

        return substitutions;
    }

    @OutputFile
    public PluginPropertiesExtension getExtension() {
        // check require properties are set
        if (this.extension.getName() == null) {
            throw new InvalidUserDataException("name is a required setting for esplugin");
        }
        if (this.extension.getDescription() == null) {
            throw new InvalidUserDataException("description is a required setting for esplugin");
        }
        if (this.extension.getClassname() == null) {
            throw new InvalidUserDataException("classname is a required setting for esplugin");
        }

        return extension;
    }

    @InputFile
    public File getDescriptorOutput() {
        return descriptorOutput;
    }

    public void setDescriptorOutput(File descriptorOutput) {
        this.descriptorOutput = descriptorOutput;
    }
}
