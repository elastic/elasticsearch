/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.vagrant;

import org.apache.commons.io.output.TeeOutputStream;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.internal.LoggingOutputStream;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.Action;
import org.gradle.api.provider.Provider;
import org.gradle.internal.logging.progress.ProgressLogger;
import org.gradle.internal.logging.progress.ProgressLoggerFactory;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.UnaryOperator;

import javax.inject.Inject;

/**
 * An helper to manage a vagrant box.
 *
 * This is created alongside a {@link VagrantExtension} for a project to manage starting and
 * stopping a single vagrant box.
 */
public class VagrantMachine {

    private final VagrantExtension extension;
    private final Provider<ReaperService> reaperServiceProvider;
    private ReaperService reaper;
    // pkg private so plugin can set this after construction
    long refs;
    private boolean isVMStarted = false;

    public VagrantMachine(VagrantExtension extension, Provider<ReaperService> reaperServiceProvider) {
        this.extension = extension;
        this.reaperServiceProvider = reaperServiceProvider;
    }

    @Inject
    protected ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected ExecOperations getExecOperations() {
        throw new UnsupportedOperationException();
    }

    public void execute(Action<VagrantExecSpec> action) {
        VagrantExecSpec vagrantSpec = new VagrantExecSpec();
        action.execute(vagrantSpec);

        Objects.requireNonNull(vagrantSpec.command);

        LoggedExec.exec(getExecOperations(), execSpec -> {
            execSpec.setExecutable("vagrant");
            File vagrantfile = extension.getVagrantfile();
            execSpec.setEnvironment(System.getenv()); // pass through env
            execSpec.environment("VAGRANT_CWD", vagrantfile.getParentFile().toString());
            execSpec.environment("VAGRANT_VAGRANTFILE", vagrantfile.getName());
            extension.getHostEnv().forEach(execSpec::environment);

            execSpec.args(vagrantSpec.command);
            if (vagrantSpec.subcommand != null) {
                execSpec.args(vagrantSpec.subcommand);
            }
            execSpec.args(extension.getBox());
            if (vagrantSpec.args != null) {
                execSpec.args(Arrays.asList(vagrantSpec.args));
            }

            UnaryOperator<String> progressHandler = vagrantSpec.progressHandler;
            if (progressHandler == null) {
                progressHandler = new VagrantProgressLogger("==> " + extension.getBox() + ": ");
            }
            OutputStream output = execSpec.getStandardOutput();
            // output from vagrant needs to be manually curated because --machine-readable isn't actually "readable"
            OutputStream progressStream = new ProgressOutputStream(vagrantSpec.command, progressHandler);
            execSpec.setStandardOutput(new TeeOutputStream(output, progressStream));
        });
    }

    // start the configuration VM if it hasn't been started yet
    void maybeStartVM() {
        if (isVMStarted) {
            return;
        }
        execute(spec -> {
            spec.setCommand("box");
            spec.setSubcommand("update");
        });

        // Destroying before every execution can be annoying while iterating on tests locally. Therefore, we provide a flag that defaults
        // to true that can be used to control whether or not to destroy any test boxes before test execution.
        boolean destroyVM = Util.getBooleanProperty("vagrant.destroy", true);
        if (destroyVM) {
            execute(spec -> {
                spec.setCommand("destroy");
                spec.setArgs("--force");
            });
        }

        // register box to be shutdown if gradle dies
        reaper = reaperServiceProvider.get();
        reaper.registerCommand(extension.getBox(), "vagrant", "halt", "-f", extension.getBox());

        // We lock the provider to virtualbox because the Vagrantfile specifies lots of boxes that only work
        // properly in virtualbox. Virtualbox is vagrant's default but its possible to change that default and folks do.
        execute(spec -> {
            spec.setCommand("up");
            spec.setArgs("--provision", "--provider", "virtualbox");
        });
        isVMStarted = true;
    }

    // stops the VM if refs are down to 0, or force was called
    void maybeStopVM(boolean force) {
        assert refs >= 1;
        this.refs--;
        if ((refs == 0 || force) && isVMStarted) {
            execute(spec -> spec.setCommand("halt"));
            reaper.unregister(extension.getBox());
        }
    }

    public static String convertLinuxPath(File rootDir, String path) {
        return "/elasticsearch/" + rootDir.toPath().relativize(Paths.get(path));
    }

    public static String convertWindowsPath(File rootDir, String path) {
        return "C:\\elasticsearch\\" + rootDir.toPath().relativize(Paths.get(path)).toString().replace('/', '\\');
    }

    public static class VagrantExecSpec {
        private String command;
        private String subcommand;
        private String[] args;
        private UnaryOperator<String> progressHandler;

        private VagrantExecSpec() {}

        public void setCommand(String command) {
            this.command = command;
        }

        public void setSubcommand(String subcommand) {
            this.subcommand = subcommand;
        }

        public void setArgs(String... args) {
            this.args = args;
        }

        /**
         * A function to translate output from the vagrant command execution to the progress line.
         *
         * The function takes the current line of output from vagrant, and returns a new
         * progress line, or {@code null} if there is no update.
         */
        public void setProgressHandler(UnaryOperator<String> progressHandler) {
            this.progressHandler = progressHandler;
        }
    }

    private class ProgressOutputStream extends LoggingOutputStream {

        private ProgressLogger progressLogger;
        private UnaryOperator<String> progressHandler;

        ProgressOutputStream(String command, UnaryOperator<String> progressHandler) {
            this.progressHandler = progressHandler;
            this.progressLogger = getProgressLoggerFactory().newOperation("vagrant");
            progressLogger.start(extension.getBox() + "> " + command, "hello");
        }

        @Override
        protected void logLine(String line) {
            String progress = progressHandler.apply(line);
            if (progress != null) {
                progressLogger.progress(progress);
            }
        }

        @Override
        public void close() {
            progressLogger.completed();
        }
    }

}
