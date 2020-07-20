/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Exec;
import org.gradle.process.BaseExecSpec;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;
import org.gradle.process.JavaExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
@SuppressWarnings("unchecked")
public class LoggedExec extends Exec {

    private static final Logger LOGGER = Logging.getLogger(LoggedExec.class);
    private Consumer<Logger> outputLogger;

    public LoggedExec() {

        if (getLogger().isInfoEnabled() == false) {
            setIgnoreExitValue(true);
            setSpoolOutput(false);
            // We use an anonymous inner class here because Gradle cannot properly snapshot this input for the purposes of
            // incremental build if we use a lambda. This ensures LoggedExec tasks that declare output can be UP-TO-DATE.
            doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    if (LoggedExec.this.getExecResult().getExitValue() != 0) {
                        try {
                            LoggedExec.this.getLogger().error("Output for " + LoggedExec.this.getExecutable() + ":");
                            outputLogger.accept(LoggedExec.this.getLogger());
                        } catch (Exception e) {
                            throw new GradleException("Failed to read exec output", e);
                        }
                        throw new GradleException(
                            String.format(
                                "Process '%s %s' finished with non-zero exit value %d",
                                LoggedExec.this.getExecutable(),
                                LoggedExec.this.getArgs(),
                                LoggedExec.this.getExecResult().getExitValue()
                            )
                        );
                    }
                }
            });
        }
    }

    public void setSpoolOutput(boolean spoolOutput) {
        final OutputStream out;
        if (spoolOutput) {
            File spoolFile = new File(getProject().getBuildDir() + "/buffered-output/" + this.getName());
            out = new LazyFileOutputStream(spoolFile);
            outputLogger = logger -> {
                try {
                    // the file may not exist if the command never output anything
                    if (Files.exists(spoolFile.toPath())) {
                        Files.lines(spoolFile.toPath()).forEach(logger::error);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("could not log", e);
                }
            };
        } else {
            out = new ByteArrayOutputStream();
            outputLogger = logger -> {
                try {
                    logger.error(((ByteArrayOutputStream) out).toString("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        setStandardOutput(out);
        setErrorOutput(out);
    }

    public static ExecResult exec(Project project, Action<ExecSpec> action) {
        return genericExec(project::exec, action);
    }

    public static ExecResult exec(ExecOperations execOperations, Action<ExecSpec> action) {
        return genericExec(execOperations::exec, action);
    }

    public static ExecResult javaexec(Project project, Action<JavaExecSpec> action) {
        return genericExec(project::javaexec, action);
    }

    private static final Pattern NEWLINE = Pattern.compile(System.lineSeparator());

    private static <T extends BaseExecSpec> ExecResult genericExec(Function<Action<T>, ExecResult> function, Action<T> action) {
        if (LOGGER.isInfoEnabled()) {
            return function.apply(action);
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            return function.apply(spec -> {
                spec.setStandardOutput(output);
                spec.setErrorOutput(output);
                action.execute(spec);
                try {
                    output.write(("Output for " + spec.getExecutable() + ":").getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (Exception e) {
            try {
                if (output.size() != 0) {
                    LOGGER.error("Exec output and error:");
                    NEWLINE.splitAsStream(output.toString("UTF-8")).forEach(s -> LOGGER.error("| " + s));
                }
            } catch (UnsupportedEncodingException ue) {
                throw new GradleException("Failed to read exec output", ue);
            }
            throw e;
        }
    }
}
