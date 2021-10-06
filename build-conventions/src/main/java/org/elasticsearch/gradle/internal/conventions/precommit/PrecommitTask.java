/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.conventions.precommit;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class PrecommitTask extends DefaultTask {

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProjectLayout().getBuildDirectory().getAsFile().get(), "markers/" + this.getName());
    }

    @TaskAction
    public void writeMarker() throws IOException {
        Files.write(getSuccessMarker().toPath(), new byte[] {}, StandardOpenOption.CREATE);
    }

    @Inject
    protected ProjectLayout getProjectLayout() {
        throw new UnsupportedOperationException();
    }
}
