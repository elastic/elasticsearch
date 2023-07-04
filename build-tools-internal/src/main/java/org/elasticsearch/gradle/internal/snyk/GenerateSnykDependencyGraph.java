/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import groovy.json.JsonOutput;

import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolvedDependency;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

public class GenerateSnykDependencyGraph extends DefaultTask {

    private static final Map<String, Object> FIXED_META_DATA = Map.of(
        "method",
        "custom gradle",
        "id",
        "gradle",
        "node",
        "v16.15.1",
        "name",
        "gradle",
        "plugin",
        "extern:gradle",
        "pluginRuntime",
        "unknown",
        "monitorGraph",
        true
    );
    private final Property<Configuration> configuration;
    private final Property<String> gradleVersion;
    private final RegularFileProperty outputFile;
    private final Property<String> projectName;
    private final Property<String> projectPath;
    private final Property<String> targetReference;
    private final Property<String> version;

    @Inject
    public GenerateSnykDependencyGraph(ObjectFactory objectFactory) {
        configuration = objectFactory.property(Configuration.class);
        gradleVersion = objectFactory.property(String.class);
        outputFile = objectFactory.fileProperty();
        projectName = objectFactory.property(String.class);
        projectPath = objectFactory.property(String.class);
        version = objectFactory.property(String.class);
        targetReference = objectFactory.property(String.class);
    }

    @TaskAction
    void resolveGraph() {
        Map<String, Object> payload = generateGradleGraphPayload();
        String jsonOutput = JsonOutput.prettyPrint(JsonOutput.toJson(payload));
        try {
            Files.writeString(
                getOutputFile().getAsFile().get().toPath(),
                jsonOutput,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            throw new GradleException("Cannot generate dependencies json file", e);
        }
    }

    private Map<String, Object> generateGradleGraphPayload() {
        Set<ResolvedDependency> firstLevelModuleDependencies = configuration.get()
            .getResolvedConfiguration()
            .getFirstLevelModuleDependencies();
        SnykDependencyGraphBuilder builder = new SnykDependencyGraphBuilder(gradleVersion.get());
        String effectiveProjectPath = projectPath.get();
        builder.walkGraph(
            (effectiveProjectPath.equals(":") ? projectName.get() : effectiveProjectPath),
            version.get(),
            firstLevelModuleDependencies
        );
        return Map.of(
            "meta",
            FIXED_META_DATA,
            "depGraphJSON",
            builder.build(),
            "target",
            buildTargetData(),
            "targetReference",
            targetReference.get(),
            "projectAttributes",
            projectAttributesData()
        );
    }

    private Map<String, List<String>> projectAttributesData() {
        return Map.of("lifecycle", List.of(version.map(v -> v.endsWith("SNAPSHOT") ? "development" : "production").get()));
    }

    private Object buildTargetData() {
        return Map.of("remoteUrl", "http://github.com/elastic/elasticsearch.git", "branch", BuildParams.getGitRevision());
    }

    @InputFiles
    public Property<Configuration> getConfiguration() {
        return configuration;
    }

    @OutputFile
    public RegularFileProperty getOutputFile() {
        return outputFile;
    }

    @Input
    public Property<String> getProjectPath() {
        return projectPath;
    }

    @Input
    public Property<String> getVersion() {
        return version;
    }

    @Input
    public Property<String> getProjectName() {
        return projectName;
    }

    @Input
    public Property<String> getGradleVersion() {
        return gradleVersion;
    }

    @Input
    public Property<String> getTargetReference() {
        return targetReference;
    }
}
