/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.stringtemplate.v4.ST;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import javax.inject.Inject;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class StringTemplateTask extends DefaultTask {

    private final ListProperty<TemplateSpec> templateSpecListProperty;
    private final DirectoryProperty outputFolder;

    @Inject
    public StringTemplateTask(ObjectFactory objectFactory) {
        templateSpecListProperty = objectFactory.listProperty(TemplateSpec.class);
        outputFolder = objectFactory.directoryProperty();
    }

    public void template(Action<TemplateSpec> spec) {
        TemplateSpec templateSpec = new TemplateSpec();
        spec.execute(templateSpec);
        templateSpecListProperty.add(templateSpec);
    }

    @Nested
    public ListProperty<TemplateSpec> getTemplates() {
        return templateSpecListProperty;
    }

    @OutputDirectory
    public DirectoryProperty getOutputFolder() {
        return outputFolder;
    }

    @TaskAction
    public void generate() {
        File outputRootFolder = getOutputFolder().getAsFile().get();
        for (TemplateSpec spec : getTemplates().get()) {
            getLogger().info("StringTemplateTask generating {}, with properties {}", spec.inputFile, spec.properties);
            try {
                ST st = new ST(Files.readString(spec.inputFile.toPath(), UTF_8), '$', '$');
                for (var entry : spec.properties.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        st.add(entry.getKey(), null);
                    } else {
                        st.add(entry.getKey(), entry.getValue());
                    }
                }
                String output = st.render();
                Files.createDirectories(outputRootFolder.toPath().resolve(spec.outputFile).getParent());
                Files.writeString(new File(outputRootFolder, spec.outputFile).toPath(), output, UTF_8);
                getLogger().info("StringTemplateTask generated {}", spec.outputFile);
            } catch (IOException e) {
                throw new GradleException("Cannot generate source from String template", e);
            }
        }
    }

    class TemplateSpec {
        private File inputFile;

        private String outputFile;

        private Map<String, String> properties;

        @InputFile
        @PathSensitive(PathSensitivity.RELATIVE)
        public File getInputFile() {
            return inputFile;
        }

        public void setInputFile(File inputFile) {
            this.inputFile = inputFile;
        }

        @Input
        public String getOutputFile() {
            return outputFile;
        }

        public void setOutputFile(String outputFile) {
            this.outputFile = outputFile;
        }

        @Input
        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }
}
