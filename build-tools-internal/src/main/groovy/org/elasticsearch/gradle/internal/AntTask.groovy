/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.apache.tools.ant.BuildListener
import org.apache.tools.ant.BuildLogger
import org.apache.tools.ant.DefaultLogger
import org.apache.tools.ant.Project
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.FileSystemOperations
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import groovy.ant.AntBuilder
import javax.inject.Inject
import java.nio.charset.Charset

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
        AntBuilder ant = new AntBuilder()

        // remove existing loggers, we add our own
        List<BuildLogger> toRemove = new ArrayList<>();
        for (BuildListener listener : ant.project.getBuildListeners()) {
            if (listener instanceof BuildLogger) {
                toRemove.add(listener);
            }
        }
        for (BuildLogger listener : toRemove) {
            ant.project.removeBuildListener(listener)
        }

        // otherwise groovy replaces System.out, and you have no chance to debug
        // ant.saveStreams = false
        /**
         * A buffer that will contain the output of the ant code run,
         * if the output was not already written directly to stdout.
         */
        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream()

        final int outputLevel = logger.isDebugEnabled() ? Project.MSG_DEBUG : Project.MSG_INFO
        final PrintStream stream = useStdout() ? System.out : new PrintStream(outputBuffer, true, Charset.defaultCharset().name())
        BuildLogger antLogger = makeLogger(stream, outputLevel)

        ant.project.addBuildListener(antLogger)
        try {
            runAnt(ant)
        } catch (Exception e) {
            // ant failed, so see if we have buffered output to emit, then rethrow the failure
            String buffer = outputBuffer.toString()
            if (buffer.isEmpty() == false) {
                logger.error("=== Ant output ===\n${buffer}")
            }
            throw e
        }
    }

    /** Runs the doAnt closure. This can be overridden by subclasses instead of having to set a closure. */
    protected abstract void runAnt(AntBuilder ant)

    /** Create the logger the ant runner will use, with the given stream for error/output. */
    protected BuildLogger makeLogger(PrintStream stream, int outputLevel) {
        return new DefaultLogger(
            errorPrintStream: stream,
            outputPrintStream: stream,
            messageOutputLevel: outputLevel)
    }

    /**
     * Returns true if the ant logger should write to stdout, or false if to the buffer.
     * The default implementation writes to the buffer when gradle info logging is disabled.
     */
    protected boolean useStdout() {
        return logger.isInfoEnabled()
    }


}
