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

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates a plugin descriptor.
 */
public class PluginPropertiesTask extends DefaultTask {

    private PluginPropertiesExtension extension;

    private File descriptorOutput = new File(getProject().getBuildDir(),
            "generated-resources/plugin-descriptor.properties");

    public PluginPropertiesTask() {
        Project project = this.getProject();
        this.extension = (PluginPropertiesExtension) project.getExtensions().getByName("esplugin");
    }

    @TaskAction
    public void performAction() {
        Project project = this.getProject();

        File templateFile = new File(project.getBuildDir(), "templates/" + descriptorOutput.getName());
        templateFile.getParentFile().mkdirs();

        try (InputStream resourceTemplate = this.getClass().getResourceAsStream("/" + descriptorOutput.getName())) {
            Files.copy(resourceTemplate, templateFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        project.copy(copyArg -> {
            copyArg.from(templateFile.getParentFile()).include(descriptorOutput.getName())
                .into(descriptorOutput.getParentFile()).expand(this.generateSubstitutions());
        });
    }

    @Input
    public Map<String, String> generateSubstitutions() {
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

        Map<String, String> substitutions = new HashMap<>();
        substitutions.put("name", this.extension.getName());
        substitutions.put("description", this.extension.getDescription());
        substitutions.put("version", this.extension.getVersion());
        substitutions.put("elasticsearchVersion", VersionProperties.getElasticsearch());
        substitutions.put("javaVersion", getProject().getConvention().getPlugin(JavaPluginConvention.class)
                .getTargetCompatibility().toString());
        substitutions.put("classname", this.extension.getClassname());
        substitutions.put("extendedPlugins", this.extension.getExtendedPlugins().stream().collect(Collectors.joining(",")));
        substitutions.put("hasNativeController", String.valueOf(this.extension.hasNativeController()));
        substitutions.put("requiresKeystore", String.valueOf(this.extension.isRequiresKeystore()));

        return substitutions;
    }

    public PluginPropertiesExtension getExtension() {
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
