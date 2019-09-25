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

package org.elasticsearch.gradle.vagrant;

import org.apache.commons.io.output.TeeOutputStream;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.LoggingOutputStream;
import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.Util;
import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.internal.logging.progress.ProgressLogger;
import org.gradle.internal.logging.progress.ProgressLoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * An helper to manage a vagrant box.
 *
 * This is created alongside a {@link VagrantExtension} for a project to manage starting and
 * stopping a single vagrant box.
 */
public class VagrantMachine {

    private final Project project;
    private final VagrantExtension extension;
    private final ReaperService reaper;
    // pkg private so plugin can set this after construction
    long refs;
    private boolean isVMStarted = false;

    public VagrantMachine(Project project, VagrantExtension extension, ReaperService reaper) {
        this.project = project;
        this.extension = extension;
        this.reaper = reaper;
    }

    @Inject
    protected ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    public void execute(Action<VagrantExecSpec> action) {
        VagrantExecSpec vagrantSpec = new VagrantExecSpec();
        action.execute(vagrantSpec);

        Objects.requireNonNull(vagrantSpec.command);

        LoggedExec.exec(project, execSpec -> {
            execSpec.setExecutable("vagrant");
            File vagrantfile = extension.getVagrantfile();
            execSpec.setEnvironment(System.getenv()); // pass through env
            execSpec.environment("VAGRANT_CWD", vagrantfile.getParentFile().toString());
            execSpec.environment("VAGRANT_VAGRANTFILE", vagrantfile.getName());
            execSpec.environment("VAGRANT_LOG", "debug");
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

    // convert the given path from an elasticsearch repo path to a VM path
    public static String convertLinuxPath(Project project, String path) {
        return "/elasticsearch/" + project.getRootDir().toPath().relativize(Paths.get(path));
    }

    public static String convertWindowsPath(Project project, String path) {
        return "C:\\elasticsearch\\" + project.getRootDir().toPath().relativize(Paths.get(path)).toString().replace('/', '\\');
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
            System.out.println(line);
        }

        @Override
        public void close() {
            progressLogger.completed();
        }
    }

}
