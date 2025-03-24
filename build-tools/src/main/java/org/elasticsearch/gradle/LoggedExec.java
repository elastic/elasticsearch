/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle;

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
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.CacheableTask;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * A wrapper around gradle's exec functionality to capture output and log on error.
 * This Task is configuration cache-compatible in contrast to Gradle's built-in
 * Exec task implementation.
 */
@SuppressWarnings("unchecked")
@CacheableTask
public abstract class LoggedExec extends DefaultTask implements FileSystemOperationsAware {

    private static final Logger LOGGER = Logging.getLogger(LoggedExec.class);
    protected FileSystemOperations fileSystemOperations;
    private ProjectLayout projectLayout;
    private ExecOperations execOperations;

    @Input
    @Optional
    abstract public ListProperty<Object> getArgs();

    @Input
    @Optional
    abstract public MapProperty<String, String> getEnvironment();

    @Internal
    abstract public MapProperty<String, String> getNonTrackedEnvironment();

    @Input
    abstract public Property<String> getExecutable();

    @Input
    @Optional
    abstract public Property<String> getStandardInput();

    @Input
    @Optional
    abstract public Property<String> getIndentingConsoleOutput();

    @Input
    @Optional
    abstract public Property<Boolean> getCaptureOutput();

    @Input
    public Provider<String> getWorkingDirPath() {
        return getWorkingDir().map(file -> {
            String relativeWorkingDir = projectLayout.getProjectDirectory().getAsFile().toPath().relativize(file.toPath()).toString();
            return relativeWorkingDir;
        });
    }

    @Internal
    abstract public Property<File> getWorkingDir();

    @Internal
    abstract public Property<Boolean> getSpoolOutput();

    private String output;

    @Inject
    public LoggedExec(
        ProjectLayout projectLayout,
        ExecOperations execOperations,
        FileSystemOperations fileSystemOperations,
        ProviderFactory providerFactory
    ) {
        this.projectLayout = projectLayout;
        this.execOperations = execOperations;
        this.fileSystemOperations = fileSystemOperations;
        getWorkingDir().convention(projectLayout.getProjectDirectory().getAsFile());
        // For now mimic default behaviour of Gradle Exec task here
        setupDefaultEnvironment(providerFactory);
        getCaptureOutput().convention(false);
        getSpoolOutput().convention(false);
    }

    /**
     * We explicitly configure the environment variables that are passed to the executed process.
     * This is required to make sure that the build cache and Gradle configuration cache is correctly configured
     * can be reused across different build invocations.
     * */
    private void setupDefaultEnvironment(ProviderFactory providerFactory) {
        getEnvironment().putAll(providerFactory.environmentVariablesPrefixedBy("GRADLE_BUILD_CACHE"));

        getNonTrackedEnvironment().putAll(providerFactory.environmentVariablesPrefixedBy("BUILDKITE"));
        getNonTrackedEnvironment().putAll(providerFactory.environmentVariablesPrefixedBy("VAULT"));
        Provider<String> javaToolchainHome = providerFactory.environmentVariable("JAVA_TOOLCHAIN_HOME");
        if (javaToolchainHome.isPresent()) {
            getEnvironment().put("JAVA_TOOLCHAIN_HOME", javaToolchainHome);
        }
        Provider<String> javaRuntimeHome = providerFactory.environmentVariable("RUNTIME_JAVA_HOME");
        if (javaRuntimeHome.isPresent()) {
            getEnvironment().put("RUNTIME_JAVA_HOME", javaRuntimeHome);
        }
        Provider<String> path = providerFactory.environmentVariable("PATH");
        if (path.isPresent()) {
            getEnvironment().put("PATH", path);
        }
    }

    @TaskAction
    public void run() {
        boolean spoolOutput = getSpoolOutput().get();
        if (spoolOutput && getCaptureOutput().get()) {
            throw new GradleException("Capturing output is not supported when spoolOutput is true.");
        }
        if (getCaptureOutput().get() && getIndentingConsoleOutput().isPresent()) {
            throw new GradleException("Capturing output is not supported when indentingConsoleOutput is configured.");
        }
        Consumer<Logger> outputLogger;
        OutputStream out;
        if (spoolOutput) {
            File spoolFile = new File(projectLayout.getBuildDirectory().dir("buffered-output").get().getAsFile(), this.getName());
            out = new LazyFileOutputStream(spoolFile);
            outputLogger = logger -> {
                try {
                    // the file may not exist if the command never output anything
                    if (Files.exists(spoolFile.toPath())) {
                        try (var lines = Files.lines(spoolFile.toPath())) {
                            lines.forEach(logger::error);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("could not log", e);
                }
            };
        } else {
            out = new ByteArrayOutputStream();
            outputLogger = getIndentingConsoleOutput().isPresent() ? logger -> {} : logger -> logger.error(byteStreamToString(out));
        }

        OutputStream finalOutputStream = getIndentingConsoleOutput().isPresent()
            ? new IndentingOutputStream(System.out, getIndentingConsoleOutput().get())
            : out;
        ExecResult execResult = execOperations.exec(execSpec -> {
            execSpec.setIgnoreExitValue(true);
            execSpec.setStandardOutput(finalOutputStream);
            execSpec.setErrorOutput(finalOutputStream);
            execSpec.setExecutable(getExecutable().get());
            execSpec.environment(getEnvironment().get());
            execSpec.environment(getNonTrackedEnvironment().get());
            if (getArgs().isPresent()) {
                execSpec.setArgs(getArgs().get());
            }
            if (getWorkingDir().isPresent()) {
                execSpec.setWorkingDir(getWorkingDir().get());
            }
            if (getStandardInput().isPresent()) {
                execSpec.setStandardInput(new ByteArrayInputStream(getStandardInput().get().getBytes(StandardCharsets.UTF_8)));
            }
        });
        int exitValue = execResult.getExitValue();

        if (exitValue == 0 && getCaptureOutput().get()) {
            output = byteStreamToString(out);
        }
        if (getLogger().isInfoEnabled() == false) {
            if (exitValue != 0) {
                try {
                    if (getIndentingConsoleOutput().isPresent() == false) {
                        getLogger().error("Output for " + getExecutable().get() + ":");
                    }
                    outputLogger.accept(getLogger());
                } catch (Exception e) {
                    throw new GradleException("Failed to read exec output", e);
                }
                throw new GradleException(
                    String.format("Process '%s %s' finished with non-zero exit value %d", getExecutable().get(), getArgs().get(), exitValue)
                );
            }
        }

    }

    private String byteStreamToString(OutputStream out) {
        return ((ByteArrayOutputStream) out).toString(StandardCharsets.UTF_8);
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
        if (getCaptureOutput().get() == false) {
            throw new GradleException(
                "Capturing output was not enabled. Use " + getName() + ".getCapturedOutput.set(true) to enable output capturing."
            );
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
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Cannot set commandline with empty list.");
        }
        getExecutable().set(args.get(0).toString());
        getArgs().set(args.subList(1, args.size()));
    }
}
