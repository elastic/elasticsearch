/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.plugin.scanner.ClassReaders;
import org.elasticsearch.gradle.plugin.scanner.NamedComponentScanner;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;
import org.objectweb.asm.ClassReader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

public abstract class GenerateNamedComponentsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateNamedComponentsTask.class);
    private static final String NAMED_COMPONENTS_FILE = "named_components.json";

    private final WorkerExecutor workerExecutor;
    private FileCollection classpath;

    @Inject
    public GenerateNamedComponentsTask(WorkerExecutor workerExecutor, ObjectFactory objectFactory, ProjectLayout projectLayout) {
        this.workerExecutor = workerExecutor;
        getOutputFile().convention(projectLayout.getBuildDirectory().file("generated-named-components/" + NAMED_COMPONENTS_FILE));
    }

    @TaskAction
    public void scanPluginClasses() {
        workerExecutor.noIsolation().submit(GenerateNamedComponentsAction.class, params -> {
            params.getClasspath().from(classpath);
            params.getOutputFile().set(getOutputFile());
        });
    }

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    public abstract static class GenerateNamedComponentsAction implements WorkAction<Parameters> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void execute() {
            Set<File> classpathFiles = getParameters().getClasspath().getFiles();

            List<ClassReader> classReaders = ClassReaders.ofPaths(classpathFiles.stream().map(File::toPath)).collect(Collectors.toList());

            NamedComponentScanner namedComponentScanner = new NamedComponentScanner();
            Map<String, Map<String, String>> namedComponentsMap = namedComponentScanner.scanForNamedClasses(classReaders);
            writeToFile(namedComponentsMap);
        }

        private void writeToFile(Map<String, Map<String, String>> namedComponentsMap) {
            try {
                String json = OBJECT_MAPPER.writeValueAsString(namedComponentsMap);
                File file = getParameters().getOutputFile().getAsFile().get();
                Path of = Path.of(file.getAbsolutePath());
                Files.writeString(of, json);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    interface Parameters extends WorkParameters {

        ConfigurableFileCollection getClasspath();

        RegularFileProperty getOutputFile();
    }
}
