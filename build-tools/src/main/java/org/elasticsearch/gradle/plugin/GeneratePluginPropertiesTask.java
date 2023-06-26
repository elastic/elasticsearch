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
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.ClassNode;

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

    public static final String PROPERTIES_FILENAME = "plugin-descriptor.properties";
    public static final String STABLE_PROPERTIES_FILENAME = "stable-plugin-descriptor.properties";
    private static final String DESCRIPTION = "Generates Elasticsearch Plugin descriptor file";

    @Inject
    public GeneratePluginPropertiesTask(ProjectLayout projectLayout) {
        setDescription(DESCRIPTION);
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
    @Optional
    public abstract Property<String> getClassname();

    @Input
    public abstract ListProperty<String> getExtendedPlugins();

    @Input
    public abstract Property<Boolean> getHasNativeController();

    @Input
    public abstract Property<Boolean> getRequiresKeystore();

    @Input
    public abstract Property<Boolean> getIsLicensed();

    @InputFiles
    public abstract ConfigurableFileCollection getModuleInfoFile();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @Input
    public abstract Property<Boolean> getIsStable();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        String classname = getClassname().getOrElse("");
        boolean stablePlugin = getIsStable().getOrElse(false);
        if (stablePlugin == false && classname.isEmpty()) {
            throw new InvalidUserDataException("classname is a required setting for esplugin");
        }
        if (stablePlugin && classname.isEmpty() == false) {
            throw new InvalidUserDataException("classname is a forbidden for stable esplugin");
        }

        Map<String, Object> props = new HashMap<>();
        props.put("name", getPluginName().get());
        props.put("description", getPluginDescription().get());
        props.put("version", getPluginVersion().get());
        props.put("elasticsearchVersion", getElasticsearchVersion().get());
        props.put("javaVersion", getJavaVersion().get());
        props.put("classname", classname);
        props.put("extendedPlugins", String.join(",", getExtendedPlugins().get()));
        props.put("hasNativeController", getHasNativeController().get());
        props.put("requiresKeystore", getRequiresKeystore().get());
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
        ClassNode visitor = new ClassNode();
        try (var inputStream = Files.newInputStream(moduleInfoSource)) {
            new ClassReader(inputStream).accept(visitor, 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return visitor.module.name;
    }

}
