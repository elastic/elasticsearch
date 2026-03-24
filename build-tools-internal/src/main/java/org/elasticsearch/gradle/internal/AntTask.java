/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.apache.tools.ant.BuildListener;
import org.apache.tools.ant.BuildLogger;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Project;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.tasks.TaskAction;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

/**
 * A task which will run ant commands.
 *
 * Logging for the task is customizable for subclasses by overriding makeLogger.
 */
public abstract class AntTask extends DefaultTask {
    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @TaskAction
    final void executeTask() {
        // Replace AntBuilder for native org.apache.tools.ant.Project
        Project antProject = new Project();
        antProject.init();

        // // remove existing loggers, we add our own
        List<BuildListener> toRemove = new ArrayList<>();
        for (BuildListener listener : antProject.getBuildListeners()) {
            if (listener instanceof BuildLogger) {
                toRemove.add(listener);
            }
        }

        for (BuildListener listener : toRemove) {
            antProject.removeBuildListener(listener);
        }

         /**
         * A buffer that will contain the output of the ant code run,
         * if the output was not already written directly to stdout.
         */

        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();

        final int outputLevel = getLogger().isDebugEnabled() ? Project.MSG_DEBUG : Project.MSG_INFO;

        PrintStream stream;

        try {
            stream = useStdout() ? System.out : new PrintStream(outputBuffer, true, Charset.defaultCharset().name());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Ant log stream", e);
        }

        BuildLogger antLogger = makeLogger(stream, outputLevel);
        antProject.addBuildListener(antLogger);

        try {
            runAnt(antProject);
        } catch (Exception e) {
            // ant failed, so see if we have buffered output to emit, then rethrow the failure
            String buffer = outputBuffer.toString();
            if (buffer.isEmpty() == false) {
                getLogger().error("=== Ant output ===\n" + buffer);
            }
            throw e;
        }
    }

    /** * Runs the ant logic.
     * Changed signature to org.apache.tools.ant.Project
     */
    protected abstract void runAnt(Project antProject);

    /** * Create the logger the ant runner will use.
     * Using Java setters.
     */

    protected BuildLogger makeLogger(PrintStream stream, int outputLevel) {
        DefaultLogger antLogger = new DefaultLogger();
        antLogger.setErrorPrintStream(stream);
        antLogger.setOutputPrintStream(stream);
        antLogger.setMessageOutputLevel(outputLevel);
        return antLogger;
    }

    protected boolean useStdout() {
        return getLogger().isInfoEnabled();
    }
}
