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
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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

        Task copyPluginPropertiesTemplate = project.getTasks().create("copyPluginPropertiesTemplate")
                .doLast((unused) -> {
                    InputStream resourceTemplate = PluginPropertiesTask.class.getResourceAsStream("/" + this.descriptorOutput.getName());
                    templateFile.getParentFile().mkdirs();
                    try {
                        System.out.println(templateFile); ;
                        Files.copy(resourceTemplate, templateFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        dependsOn(copyPluginPropertiesTemplate);
        extension = project.getExtensions().create("esplugin", PluginPropertiesExtension.class, project);
        project.afterEvaluate((unused) -> {
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

            from(templateFile.getParentFile()).include(this.descriptorOutput.getName());
            into(this.descriptorOutput.getParentFile());
            Map<String, String> properties = this.generateSubstitutions();
            expand(properties);
            this.getInputs().properties(properties);
        });

    }

    private Map<String, String> generateSubstitutions() {
        Map<String, String> substitutions = new HashMap<>();
        substitutions.put("name", this.extension.getName());
        substitutions.put("description", this.extension.getDescription());
        substitutions.put("version", this.extension.getVersion());
        substitutions.put("elasticsearchVersion", Version.fromString(VersionProperties.getElasticsearch()).toString());

        //TODO (Raf): No getter for project.getTargetCompatibility()? What's the alternative option for below line?
        substitutions.put("javaVersion", String.valueOf(this.getProject().getVersion()));
        substitutions.put("classname", this.extension.getClassname());

        //TODO (Raf): Should we worry about possible NPE on getExtendedPlugins() here?
        substitutions.put("extendedPlugins", this.extension.getExtendedPlugins().stream().collect(Collectors.joining(",")));
        substitutions.put("hasNativeController", String.valueOf(this.extension.isHasNativeController()));
        substitutions.put("requiresKeystore", String.valueOf(this.extension.isRequiresKeystore()));

        return substitutions;
    }

    @Input
    public PluginPropertiesExtension getExtension() {
        return extension;
    }

    public void setExtension(PluginPropertiesExtension extension) {
        this.extension = extension;
    }

    @InputFile
    public File getDescriptorOutput() {
        return descriptorOutput;
    }

    public void setDescriptorOutput(File descriptorOutput) {
        this.descriptorOutput = descriptorOutput;
    }
}
