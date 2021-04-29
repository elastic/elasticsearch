/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.apache.tools.ant.BuildListener;
import org.apache.tools.ant.BuildLogger;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Project;
import org.gradle.api.AntBuilder;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.TaskAction;
import org.gradle.testfixtures.ProjectBuilder;

import javax.inject.Inject;
import java.nio.charset.Charset;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;

/**
 * A task which will run ant commands.
 *
 * Logging for the task is customizable for subclasses by overriding makeLogger.
 */
public abstract class AntTask extends DefaultTask {

    /**
     * A buffer that will contain the output of the ant code run,
     * if the output was not already written directly to stdout.
     */
    public final ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @TaskAction
    final void executeTask() throws UnsupportedEncodingException {
        AntBuilder ant = ProjectBuilder.builder().build().getAnt();
        Logger taskLogger = this.getLogger();

        // remove existing loggers, we add our own
        List<BuildLogger> toRemove = new ArrayList<>();
        for (BuildListener listener : ant.getProject().getBuildListeners()) {
            if (listener instanceof BuildLogger) {
                toRemove.add((BuildLogger) listener);
            }
        }
        for (BuildLogger listener : toRemove) {
            ant.getProject().removeBuildListener(listener);
        }

        // otherwise groovy replaces System.out, and you have no chance to debug
        // ant.saveStreams = false

        final int outputLevel = taskLogger.isDebugEnabled() ? Project.MSG_DEBUG : Project.MSG_INFO;
        final PrintStream stream = useStdout() ? System.out : new PrintStream(outputBuffer, true, Charset.defaultCharset().name());
        BuildLogger antLogger = makeLogger(stream, outputLevel);

        ant.getProject().addBuildListener(antLogger);
        try {
            runAnt(ant);
        } catch (Exception e) {
            // ant failed, so see if we have buffered output to emit, then rethrow the failure
            String buffer = outputBuffer.toString();
            if (buffer.isEmpty() == false) {
                taskLogger.error("=== Ant output ===\n${buffer}");
            }
            throw e;
        }
    }

    /** Runs the doAnt closure. This can be overridden by subclasses instead of having to set a closure. */
    protected abstract void runAnt(AntBuilder ant);

    /** Create the logger the ant runner will use, with the given stream for error/output. */
    protected BuildLogger makeLogger(PrintStream stream, int outputLevel) {
        DefaultLogger dl = new DefaultLogger();
        dl.setErrorPrintStream(stream);
        dl.setOutputPrintStream(stream);
        dl.setMessageOutputLevel(outputLevel);

        return dl;
    }

    /**
     * Returns true if the ant logger should write to stdout, or false if to the buffer.
     * The default implementation writes to the buffer when gradle info logging is disabled.
     */
    protected boolean useStdout() {
        return this.getLogger().isInfoEnabled();
    }

}
