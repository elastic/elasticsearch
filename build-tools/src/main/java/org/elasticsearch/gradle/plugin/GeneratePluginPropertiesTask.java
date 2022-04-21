/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import groovy.text.SimpleTemplateEngine;
import groovy.text.Template;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

public abstract class GeneratePluginPropertiesTask extends DefaultTask {

    private static final String PROPERTIES_FILENAME = "plugin-descriptor.properties";

    @Inject
    public GeneratePluginPropertiesTask(ProjectLayout projectLayout) {
        setDescription("Generate " + PROPERTIES_FILENAME);
        getOutputFile().convention(projectLayout.getBuildDirectory().file("generated-descriptor/" + PROPERTIES_FILENAME));
    }

    @Input
    public abstract Property<String> getPluginName();

    @Input
    public abstract Property<String> getPluginDescription();

    @Input
    public abstract Property<String> getPluginVersion();

    @Input
    public abstract Property<String> getElasticsearchVersion();

    @Input
    public abstract Property<String> getJavaVersion();

    @Input
    public abstract Property<String> getClassname();

    @Input
    public abstract ListProperty<String> getExtendedPlugins();

    @Input
    public abstract Property<Boolean> getHasNativeController();

    @Input
    public abstract Property<Boolean> getRequiresKeystore();

    @Input
    public abstract Property<PluginType> getPluginType();

    @Input
    public abstract Property<String> getJavaOpts();

    @Input
    public abstract Property<Boolean> getIsLicensed();

    @InputFiles
    public abstract ConfigurableFileCollection getModuleInfoFile();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put("name", getPluginName().get());
        props.put("description", getPluginDescription().get());
        props.put("version", getPluginVersion().get());
        props.put("elasticsearchVersion", getPluginVersion().get());
        props.put("javaVersion", getJavaVersion().get());
        PluginType pluginType = getPluginType().get();
        props.put("classname", pluginType.equals(PluginType.BOOTSTRAP) ? "" : getClassname());
        props.put("extendedPlugins", String.join(",", getExtendedPlugins().get()));
        props.put("hasNativeController", getHasNativeController().get());
        props.put("requiresKeystore", getRequiresKeystore().get());
        props.put("type", pluginType.toString());
        props.put("javaOpts", getJavaOpts().get());
        props.put("licensed", getIsLicensed().get());
        props.put("modulename", findModuleName());

        SimpleTemplateEngine engine = new SimpleTemplateEngine();
        Path outputFile = getOutputFile().get().getAsFile().toPath();
        Files.createDirectories(outputFile.getParent());
        try (
            var inputStream = GeneratePluginPropertiesTask.class.getResourceAsStream("/" + PROPERTIES_FILENAME);
            var reader = new BufferedReader(new InputStreamReader(inputStream));
            var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
        ) {
            Template template = engine.createTemplate(reader);
            template.make(props).writeTo(writer);
        }
    }

    private String findModuleName() {
        if (getModuleInfoFile().isEmpty()) {
            return "";
        }
        Path moduleInfoSource = getModuleInfoFile().getSingleFile().toPath();
        String moduleName = null;
        try (var reader = Files.newBufferedReader(moduleInfoSource, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("module")) {
                    // This is a simple and hacky way to extract the module name from the module declaration.
                    // We could properly parse the entire file, but the module keyword is unique in the file, and
                    // the module name is guaranteed to not have spaces, so this is much simpler and quicker.
                    moduleName = line.split(" ")[1];
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (moduleName == null) {
            throw new RuntimeException("Module name missing in " + moduleInfoSource);
        }

        return moduleName;
    }
}
