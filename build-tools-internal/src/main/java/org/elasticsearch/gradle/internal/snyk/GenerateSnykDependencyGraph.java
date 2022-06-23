/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import groovy.json.JsonOutput;

import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolvedDependency;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

public class GenerateSnykDependencyGraph extends DefaultTask {

    private final Property<Configuration> configuration;
    private final Property<String> projectName;
    private final Property<String> projectPath;
    private final Property<String> version;
    private final Property<String> gradleVersion;
    private final SnykGraph graph = new SnykGraph("root-node");
    private final RegularFileProperty outputFile;
    private BuildLayout buildLayout;

    private String jsonOutput = null;

    @Inject
    public GenerateSnykDependencyGraph(ObjectFactory objectFactory, BuildLayout buildLayout) {
        configuration = objectFactory.property(Configuration.class);
        projectName = objectFactory.property(String.class);
        projectPath = objectFactory.property(String.class);
        version = objectFactory.property(String.class);
        gradleVersion = objectFactory.property(String.class);
        outputFile = objectFactory.fileProperty();
        this.buildLayout = buildLayout;
    }

    @Internal
    public SnykGraph getGraph() {
        return graph;
    }

    @TaskAction
    void resolveGraph() {
        Map<String, Object> payload = generateGradleGraphPayload();
        jsonOutput = JsonOutput.prettyPrint(JsonOutput.toJson(payload));
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
        SnykModelBuilder builder = new SnykModelBuilder(gradleVersion.get());
        builder.walkGraph((projectPath.equals(":") ? projectName.get() : projectPath.get()), version.get(), firstLevelModuleDependencies);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("meta", generateMetaData());
        payload.put("depGraphJSON", builder.build());
        payload.put("target", buildTargetData());
        return payload;
    }

    private Object buildTargetData() {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("remoteUrl", "http://github.com/elastic/elasticsearch.git");
        target.put("branch", GitInfo.gitInfo(buildLayout.getRootDirectory()).getRevision());
        return target;
    }

    private Map<String, Object> generateMetaData() {
        Map<String, Object> metaData = new LinkedHashMap<>();
        metaData.put("method", "custom gradle");
        metaData.put("id", "gradle");
        metaData.put("node", "v16.15.1");
        metaData.put("name", "gradle");
        metaData.put("plugin", "extern:gradle");
        metaData.put("pluginRuntime", "unknown");
        metaData.put("monitorGraph", true);
        // metaData.put("version", version.get());
        return metaData;
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

}
