/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.util.FileUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.File;
import java.util.stream.Collectors;

abstract class GenerateProviderManifest extends DefaultTask {

    @Inject
    public GenerateProviderManifest() {
    }

    @Classpath
    @InputFiles
    abstract public ConfigurableFileCollection getProviderImplClasspath();

    @Input
    abstract public Property<String> getManifestName();
    @OutputDirectory
    abstract DirectoryProperty getOutputDir();

    @TaskAction
    void generateManifest() {
        File manifestFile = getOutputDir().file(getManifestName().get()).get().getAsFile();
        FileUtils.write(manifestFile, generateManifestContent(), "UTF-8");
    }

    private String generateManifestContent() {
        return getProviderImplClasspath().getFiles().stream().map(File::getName).sorted().collect(Collectors.joining("\n"));
    }
}
