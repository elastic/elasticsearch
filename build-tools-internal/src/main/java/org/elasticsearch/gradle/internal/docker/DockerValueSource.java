/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docker;

import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.util.List;

import javax.inject.Inject;

public abstract class DockerValueSource implements ValueSource<DockerResult, DockerValueSource.Parameters> {
    public interface OutputFilter {
        String filter(String input);
    }

    interface Parameters extends ValueSourceParameters {
        ListProperty<String> getArgs();

        Property<OutputFilter> getOutputFilter();
    }

    @Inject
    abstract protected ExecOperations getExecOperations();

    @Override
    public DockerResult obtain() {
        return runCommand(getParameters().getArgs().get());
    }

    /**
     * Runs a command and captures the exit code, standard output and standard error.
     *
     * @param args the command and any arguments to execute
     * @return a object that captures the result of running the command. If an exception occurring
     * while running the command, or the process was killed after reaching the 10s timeout,
     * then the exit code will be -1.
     */
    private DockerResult runCommand(List args) {
        if (args.size() == 0) {
            throw new IllegalArgumentException("Cannot execute with no command");
        }

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        final ExecResult execResult = getExecOperations().exec(spec -> {
            // The redundant cast is to silence a compiler warning.
            spec.setCommandLine(args);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stderr);
            spec.setIgnoreExitValue(true);
        });
        return new DockerResult(execResult.getExitValue(), filtered(stdout.toString()), stderr.toString());
    }

    private String filtered(String input) {
        return getParameters().getOutputFilter().get().filter(input);
    }

}
