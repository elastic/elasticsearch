/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import groovy.lang.Closure;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

public abstract class LoggedExecFixture extends DefaultTask {

    private final TaskProvider<LoggedExecFixtureStop> stopTask;
    private final ProjectLayout projectLayout;
    private final FileSystemOperations fileSystemOperations;
    private WorkerExecutor workerExecutor;

    @Input
    @Optional
    abstract public ListProperty<Object> getArgs();

    @Input
    @Optional
    abstract public MapProperty<String, Object> getEnvironment();

    @Input
    abstract public Property<String> getExecutable();

    @Input
    abstract public Property<File> getWorkingDir();

    @Internal
    abstract public Property<Integer> getMaxWaitInSeconds();

    @Internal
    abstract public Property<Boolean> getSpawn();

    @Inject
    public LoggedExecFixture(ProjectLayout projectLayout, FileSystemOperations fileSystemOperations, WorkerExecutor workerExecutor) {
        this.projectLayout = projectLayout;
        this.fileSystemOperations = fileSystemOperations;
        this.workerExecutor = workerExecutor;
        getMaxWaitInSeconds().convention(30);
        getWorkingDir().set(getCwd());
        // getCleanSpec().convention(spec -> {
        // spec.delete(getPidFile());
        // spec.delete(getWorkingDir().get());
        // });
        getWaitingCondition().convention((fixture) -> {
            try {
                URL url = new URL("http://" + fixture.getAddressAndPort());
                BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
                while ((in.readLine()) != null)
                    in.close();
            } catch (Exception e) {
                return false;
            }
            return true;
        });
        stopTask = createStopTask();
    }

    @TaskAction
    public void exec() {
        WorkQueue workQueue = workerExecutor.noIsolation();
        workQueue.submit(RunExecFixture.class, parameters -> {
            parameters.getWorkingDir().set(getWorkingDir().get());
            parameters.getArgs().set(getArgs().get().stream().map(arg -> arg.toString()).collect(Collectors.toList()));
            parameters.getExecutable().set(getExecutable().get().toString());
            Map<String, String> effectiveEnv = new HashMap<>();
            getEnvironment().get().entrySet().forEach(entry -> effectiveEnv.put(entry.getKey().toString(), entry.getValue().toString()));
            effectiveEnv.replaceAll((s, o) -> o.toString());
            parameters.getEnvironment().set(effectiveEnv);
        });
        if (getSpawn().convention(true).get()) {
            doWaitForFixture();
        }
    }

    private void doWaitForFixture() {
        long end = System.currentTimeMillis() + getMaxWaitInSeconds().get() * 1000;
        while (System.currentTimeMillis() < end && callWaitingCondition() == false) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (callWaitingCondition() == false) {
            throw new GradleException("Timeout waiting for " + getPath() + " waiting condition fullfilled.");
        }
    }

    /** Adds a task to kill an elasticsearch node with the given pidfile */
    private TaskProvider<LoggedExecFixtureStop> createStopTask() {
        TaskProvider<LoggedExecFixtureStop> stop = getProject().getTasks().register(getName() + "#stop", LoggedExecFixtureStop.class);
        stop.configure(loggedExecFixtureStop -> loggedExecFixtureStop.setFixture(LoggedExecFixture.this));
        finalizedBy(stop);
        return stop;
    }

    private boolean callWaitingCondition() {
        try {
            return getWaitingCondition().get().call(this);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * A path relative to the build dir that all configuration and runtime files
     * will live in for this fixture
     */
    @Internal
    protected File getBaseDir() {
        return projectLayout.getBuildDirectory().dir("fixtures/" + getName()).get().getAsFile();
    }

    @Input
    public abstract Property<WaitingCondition> getWaitingCondition();

    /** Returns the working directory for the process. Defaults to "cwd" inside baseDir. */
    @Internal
    protected File getCwd() {
        return new File(getBaseDir(), "cwd");
    }

    /** Returns the file the process writes its pid to. Defaults to "pid" inside baseDir. */
    @Internal
    protected File getPidFile() {
        return new File(getBaseDir(), "pid");
    }

    /** Reads the pid file and returns the process' pid */
    @Internal
    int getPid() {
        try {
            return Integer.parseInt(FileUtils.readFileToString(getPidFile()).trim());
        } catch (IOException exception) {
            throw new GradleException("Cannot read pid file", exception);
        }
    }

    /** Returns the file the process writes its bound ports to. Defaults to "ports" inside baseDir. */
    @Internal
    protected File getPortsFile() {
        return new File(getBaseDir(), "ports");
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    @Internal
    String getAddressAndPort() {
        try {
            return FileUtils.readLines(getPortsFile(), "UTF-8").get(0);
        } catch (IOException exception) {
            throw new GradleException("Cannot read address and port", exception);
        }
    }

    /** Returns a file that wraps around the actual command when {@code spawn == true}. */
    @Internal
    protected File getWrapperScript() {
        return new File(getCwd(), Os.isFamily(Os.FAMILY_WINDOWS) ? "run.bat" : "run");
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getFailureMarker() {
        return new File(getCwd(), "run.failed");
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getRunLog() {
        return new File(getCwd(), "run.log");
    }

    @FunctionalInterface
    public interface WaitingCondition {
        boolean call(LoggedExecFixture fixture);
    }

    public void waitingCondition(Closure<Boolean> waitingConditionClosure) {
        getWaitingCondition().set(fixture -> waitingConditionClosure.call(fixture));
    }

    public abstract static class RunExecFixture implements WorkAction<RunExecFixtureParameters> {

        private ExecOperations execOperations;
        private FileSystemOperations fileSystemOperations;

        @Inject
        public RunExecFixture(ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
            this.execOperations = execOperations;
            this.fileSystemOperations = fileSystemOperations;
        }

        @Override
        public void execute() {
            execOperations.exec(execSpec -> {
                execSpec.setExecutable(getParameters().getExecutable().get());
                execSpec.setEnvironment(getParameters().getEnvironment().get());
                if (getParameters().getArgs().isPresent()) {
                    execSpec.setArgs(getParameters().getArgs().get());
                }
                if (getParameters().getWorkingDir().isPresent()) {
                    File workingDir = getParameters().getWorkingDir().get().getAsFile();
                    workingDir.mkdirs();
                    execSpec.setWorkingDir(workingDir);
                }
            });
        }
    }

    public interface RunExecFixtureParameters extends WorkParameters {
        RegularFileProperty getWorkingDir();

        ListProperty<String> getArgs();

        MapProperty<String, Object> getEnvironment();

        Property<String> getExecutable();
    }
}
