/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.io.output.TeeOutputStream;
import org.elasticsearch.gradle.util.SerializableConsumer;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.WorkResult;
import org.gradle.process.BaseExecSpec;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;
import org.gradle.process.JavaExecSpec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
@SuppressWarnings("unchecked")
public abstract class LoggedExec extends DefaultTask implements FileSystemOperationsAware {

    private static final Logger LOGGER = Logging.getLogger(LoggedExec.class);
    private SerializableConsumer<Logger> outputLogger;
    protected FileSystemOperations fileSystemOperations;
    private ProjectLayout projectLayout;
    private ExecOperations execOperations;
    private boolean spoolOutput;

    @Input
    @Optional
    abstract public ListProperty<Object> getArgs();

    @Input
    @Optional
    abstract public MapProperty<String, String> getEnvironment();

    @Input
    abstract public Property<String> getExecutableProperty();

    @Input
    @Optional
    abstract public Property<String> getStandardInput();

    @Input
    @Optional
    abstract public Property<String> getOutputIndenting();

    @Input
    @Optional
    abstract public Property<Boolean> getCaptureOutput();

    @Input
    abstract public Property<File> getWorkingDir();

    private String output;

    @Inject
    public LoggedExec(ProjectLayout projectLayout, ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
        this.projectLayout = projectLayout;
        this.execOperations = execOperations;
        this.fileSystemOperations = fileSystemOperations;
        getWorkingDir().convention(projectLayout.getProjectDirectory().getAsFile());
        // For now mimic default behaviour of Gradle Exec task here
        getEnvironment().putAll(System.getenv());
        getCaptureOutput().convention(false);
    }

    @TaskAction
    public void run() {
        OutputStream out = getCaptureOutput().get() ? new ByteArrayOutputStream() : new NullOutputStream();
        OutputStream effectiveOutputStream = out;
        if (spoolOutput) {
            File spoolFile = new File(projectLayout.getBuildDirectory().dir("buffered-output").get().getAsFile(), this.getName());
            effectiveOutputStream = new TeeOutputStream(out, new LazyFileOutputStream(spoolFile));
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
            outputLogger = logger -> { logger.error(byteStreamToString(out)); };
        }

        OutputStream effectiveOutStream = effectiveOutputStream;
        ExecResult execResult = execOperations.exec(execSpec -> {
            execSpec.setIgnoreExitValue(true);
            execSpec.setStandardOutput(
                getOutputIndenting().isPresent()
                    ? new TeeOutputStream(new IndentingOutputStream(System.out, getOutputIndenting().get()), effectiveOutStream)
                    : effectiveOutStream
            );
            execSpec.setErrorOutput(
                getOutputIndenting().isPresent()
                    ? new TeeOutputStream(new IndentingOutputStream(System.err, getOutputIndenting().get()), effectiveOutStream)
                    : effectiveOutStream
            );
            execSpec.setExecutable(getExecutableProperty().get());
            execSpec.setEnvironment(getEnvironment().get());
            if (getArgs().isPresent()) {
                execSpec.setArgs(getArgs().get());
            }
            if (getWorkingDir().isPresent()) {
                execSpec.setWorkingDir(getWorkingDir().get());
            }
            if (getStandardInput().isPresent()) {
                try {
                    execSpec.setStandardInput(new ByteArrayInputStream(getStandardInput().get().getBytes("UTF-8")));
                } catch (UnsupportedEncodingException e) {
                    throw new GradleException("Cannot set standard input", e);
                }
            }
        });
        int exitValue = execResult.getExitValue();

        if (exitValue == 0 && getCaptureOutput().get()) {
            output = byteStreamToString(out);
        }
        if (getLogger().isInfoEnabled() == false) {
            // We use an anonymous inner class here because Gradle cannot properly snapshot this input for the purposes of
            // incremental build if we use a lambda. This ensures LoggedExec tasks that declare output can be UP-TO-DATE.

            if (exitValue != 0) {
                try {
                    LoggedExec.this.getLogger().error("Output for " + getExecutableProperty().get() + ":");
                    outputLogger.accept(LoggedExec.this.getLogger());
                } catch (Exception e) {
                    throw new GradleException("Failed to read exec output", e);
                }
                throw new GradleException(
                    String.format(
                        "Process '%s %s' finished with non-zero exit value %d",
                        LoggedExec.this.getExecutableProperty().get(),
                        LoggedExec.this.getArgs().get(),
                        exitValue
                    )
                );
            }
        }

    }

    private String byteStreamToString(OutputStream out) {
        return ((ByteArrayOutputStream) out).toString(StandardCharsets.UTF_8);
    }

    public void setSpoolOutput(boolean spoolOutput) {
        this.spoolOutput = spoolOutput;
    }

    public static ExecResult exec(ExecOperations execOperations, Action<ExecSpec> action) {
        return genericExec(execOperations::exec, action);
    }

    public static ExecResult javaexec(ExecOperations project, Action<JavaExecSpec> action) {
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
            if (output.size() != 0) {
                LOGGER.error("Exec output and error:");
                NEWLINE.splitAsStream(output.toString(StandardCharsets.UTF_8)).forEach(s -> LOGGER.error("| " + s));
            }
            throw e;
        }
    }

    @Override
    public WorkResult delete(Object... objects) {
        return fileSystemOperations.delete(d -> d.delete(objects));
    }

    @Internal
    public String getOutput() {
        if(getCaptureOutput().get() == false) {
            throw new GradleException("Capturing output was not enabled. Use " + getName() +
                    ".getCapturedOutput.set(true) to enable output capturing");
        }
        return output;
    }

    private static class IndentingOutputStream extends OutputStream {

        public final byte[] indent;
        private final OutputStream delegate;

        IndentingOutputStream(OutputStream delegate, Object version) {
            this.delegate = delegate;
            indent = (" [" + version + "] ").getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void write(int b) throws IOException {
            int[] arr = { b };
            write(arr, 0, 1);
        }

        public void write(int[] bytes, int offset, int length) throws IOException {
            for (int i = 0; i < bytes.length; i++) {
                delegate.write(bytes[i]);
                if (bytes[i] == '\n') {
                    delegate.write(indent);
                }
            }
        }
    }

    public void setExecutable(String executable) {
        getExecutableProperty().set(executable);
    }

    public void args(Object... args) {
        args(List.of(args));
    }

    public void args(List<Object> args) {
        getArgs().addAll(args);
    }

    public void commandLine(Object... args) {
        commandLine(List.of(args));
    }

    public void commandLine(List<Object> args) {
        if (args.size() == 0) {
            throw new IllegalArgumentException("Cannot set commandline with of entry size 0");
        }
        getExecutableProperty().set(args.get(0).toString());
        getArgs().set(args.subList(1, args.size()));
    }
}
