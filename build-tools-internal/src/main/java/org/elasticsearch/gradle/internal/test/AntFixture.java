/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.ExecTask;
import org.apache.tools.ant.taskdefs.Get;
import org.apache.tools.ant.taskdefs.WaitFor;
import org.apache.tools.ant.taskdefs.condition.And;
import org.apache.tools.ant.taskdefs.condition.Or;
import org.apache.tools.ant.taskdefs.condition.ResourceExists;
import org.apache.tools.ant.types.Commandline;
import org.apache.tools.ant.types.Environment;
import org.apache.tools.ant.types.resources.FileResource;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.internal.AntFixtureStop;
import org.elasticsearch.gradle.internal.AntTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskProvider;
import org.apache.tools.ant.taskdefs.condition.Condition;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * A fixture for integration tests which runs in a separate process launched by Ant.
 */
public abstract class AntFixture extends AntTask {
    @Internal
    private String executable;
    private final List<Object> arguments = new ArrayList<>();
    /**
     * Environment variables for the fixture process. The value can be any object, which
     * will have toString() called at execution time.
     */
    private final Map<String, Object> environment = new HashMap<>();
    private final ProjectLayout projectLayout;
    private final ProviderFactory providerFactory;
    /**
     * A flag to indicate whether the fixture should be run in the foreground, or spawned.
     * It is protected so subclasses can override (eg RunTask).
     */
    protected boolean spawn = true;
    @Internal
    protected int maxWaitInSeconds = 30;
    /** A flag to indicate whether the command should be executed from a shell. */
    @Internal
    protected boolean useShell = false;

    // Replaced Closure for BiFunction compatible with Java
    private BiFunction<AntFixture, Project, Boolean> waitCondition = this::defaultWaitCondition;

    @Inject
    public AntFixture(ProjectLayout projectLayout, ProviderFactory providerFactory) {
        this.projectLayout = projectLayout;
        this.providerFactory = providerFactory;
        TaskProvider<AntFixtureStop> stopTask = createStopTask();
        finalizedBy(stopTask);
    }

    @Override
    protected void runAnt(Project antProject) {
        getFileSystemOperations().delete(spec -> spec.delete(getBaseDir()));
        getCwd().mkdirs();

        String realExecutable;
        List<Object> realArgs = new ArrayList<>();

        if (useShell || spawn) {
            if (OS.current() == OS.WINDOWS) {
                realExecutable = "cmd";
                realArgs.add("/C");
                realArgs.add("\"");
            } else {
                realExecutable = "sh";
            }
        } else {
            realExecutable = executable;
            realArgs.addAll(arguments);
        }

        if (spawn) {
            writeWrapperScript(executable);
            realArgs.add(getWrapperScript().getAbsolutePath());
            realArgs.addAll(arguments);
        }

        if (OS.current() == OS.WINDOWS && (useShell || spawn)) {
            realArgs.add("\"");
        }

        getCommandString().lines().forEach(getLogger()::info);

        // --- Configuración de ExecTask (reemplaza ant.exec) ---
        ExecTask exec = new ExecTask();
        exec.setProject(antProject);
        exec.setExecutable(realExecutable);
        exec.setSpawn(spawn);
        exec.setDir(getCwd());
        exec.setTaskName(getName());

        environment.forEach((key, value) -> {
            Environment.Variable var = new Environment.Variable();
            var.setKey(key);
            var.setValue(value.toString());
            exec.addEnv(var);
        });

        realArgs.forEach(arg -> {
            Commandline.Argument a = exec.createArg();
            a.setValue(arg.toString());
        });
        exec.execute();

        // --- Configuración de WaitFor (reemplaza ant.waitfor) ---
        String failedProp = "failed" + getName();
        WaitFor wait = new WaitFor();
        wait.setProject(antProject);
        wait.setMaxWait(maxWaitInSeconds);

        // Configuración de la unidad de tiempo para MaxWait (segundos)
        WaitFor.Unit maxWaitUnit = new WaitFor.Unit();
        maxWaitUnit.setValue("second");
        wait.setMaxWaitUnit(maxWaitUnit);

        wait.setCheckEvery(500);

        // Configuración de la unidad de tiempo para CheckEvery (milisegundos)
        WaitFor.Unit checkEveryUnit = new WaitFor.Unit();
        checkEveryUnit.setValue("millisecond");
        wait.setCheckEveryUnit(checkEveryUnit);

        wait.setTimeoutProperty(failedProp);

        Or orCondition = new Or();
        orCondition.setProject(antProject);

        And andCondition = new And();
        andCondition.setProject(antProject);

        // Para el marcador de fallo
        ResourceExists failMarker = new ResourceExists();
        failMarker.setProject(antProject); // Siempre asocia el proyecto
        FileResource failureFileRes = new FileResource();
        failureFileRes.setFile(getFailureMarker());
        failMarker.add(failureFileRes); // Usa add() o setResource() según la versión

        // Para el PID y Ports (dentro del And)
        ResourceExists pidRes = new ResourceExists();
        FileResource pidFileRes = new FileResource();
        pidFileRes.setFile(getPidFile());
        pidRes.add(pidFileRes);
        andCondition.add(pidRes);

        ResourceExists portsRes = new ResourceExists();
        FileResource portsFileRes = new FileResource();
        portsFileRes.setFile(getPortsFile());
        portsRes.add(portsFileRes);
        andCondition.add(portsRes);

        orCondition.add((Condition) andCondition);
        wait.add((Condition) orCondition);
        wait.execute();

        if (antProject.getProperty(failedProp) != null || getFailureMarker().exists()) {
            fail("Failed to start " + getName());
        }

        boolean success;
        try {
            success = waitCondition.apply(this, antProject);
        } catch (Exception e) {
            String msg = "Wait condition caught exception for " + getName();
            getLogger().error(msg, e);
            fail(msg, e);
            return;
        }

        if (!success) {
            fail("Wait condition failed for " + getName());
        }
    }

    private boolean defaultWaitCondition(AntFixture fixture, Project antProject) {
        File tmpFile = new File(fixture.getCwd(), "wait.success");
        try {
            Get getTask = new Get();
            getTask.setProject(antProject);
            getTask.setSrc(new URL("http://" + fixture.getAddressAndPort()));
            getTask.setDest(tmpFile);
            getTask.setIgnoreErrors(true);
            getTask.setRetries(10);
            getTask.execute();
        } catch (Exception e) {
            return false;
        }
        return tmpFile.exists();
    }

    /**
     * Writes a script to run the real executable, so that stdout/stderr can be captured.
     * TODO: this could be removed if we do use our own ProcessBuilder and pump output from the process
     */
    private void writeWrapperScript(String executable) {
        getWrapperScript().getParentFile().mkdirs();
        String argsPasser = OS.current() == OS.WINDOWS ? "%*" : "\"$@\"";
        String exitMarker = OS.current() == OS.WINDOWS
            ? "\r\n if \"%errorlevel%\" neq \"0\" ( type nul >> run.failed )"
            : "; if [ $? != 0 ]; then touch run.failed; fi";

        String content = "\"" + executable + "\" " + argsPasser + " > run.log 2>&1 " + exitMarker;
        try {
            Files.writeString(getWrapperScript().toPath(), content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new GradleException("Failed to write wrapper script", e);
        }
    }
    /** Returns a debug string used to log information about how the fixture was run. */
    @Internal
    protected String getCommandString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append(getName()).append(" configuration:\n");
        sb.append("-----------------------------------------\n");
        sb.append("  cwd: ").append(getCwd()).append("\n");
        sb.append("  command: ")
            .append(getExecutable())
            .append(" ")
            .append(String.join(" ", arguments.stream().map(Object::toString).toList()))
            .append("\n");
        sb.append("  environment:\n");

        environment.forEach((k, v) -> sb.append("    ").append(k).append(": ").append(v).append("\n"));

        if (spawn) {
            sb.append("\n  [").append(getWrapperScript().getName()).append("]\n");
            try {
                Files.readAllLines(getWrapperScript().toPath(), StandardCharsets.UTF_8)
                    .forEach(line -> sb.append("    ").append(line).append("\n"));
            } catch (IOException e) {
                sb.append("    (could not read wrapper script: ").append(e.getMessage()).append(")\n");
            }
        }
        return sb.toString();
    }

    // --- Getters, Setters y utilidades (Mantenidos de la lógica original) ---
    @Internal
    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    /**
     * A path relative to the build dir that all configuration and runtime files
     * will live in for this fixture
     */
    @Internal
    protected File getBaseDir() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "fixtures/" + getName());
    }

    /** Returns the working directory for the process. Defaults to "cwd" inside baseDir. */
    @Internal
    protected File getCwd() {
        return new File(getBaseDir(), "cwd");
    }

    /** Returns the file the process writes its pid to. Defaults to "pid" inside baseDir. */
    @Internal
    public File getPidFile() {
        return new File(getBaseDir(), "pid");
    }

    /** Returns the file the process writes its bound ports to. Defaults to "ports" inside baseDir. */
    @Internal
    protected File getPortsFile() {
        return new File(getBaseDir(), "ports");
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    @Internal
    public String getAddressAndPort() {
        try {
            return Files.readAllLines(getPortsFile().toPath(), StandardCharsets.UTF_8).get(0);
        } catch (IOException e) {
            throw new GradleException("Failed to read ports file", e);
        }
    }

    @Internal
    protected File getFailureMarker() {
        return new File(getCwd(), "run.failed");
    }

    /** Fail the build with the given message, and logging relevant info*/
    private void fail(String msg, Exception... suppressed) {
        getLogger().error(getName() + " output failure: " + msg);
        GradleException toThrow = new GradleException(msg);
        for (Exception e : suppressed) {
            toThrow.addSuppressed(e);
        }
        throw toThrow;
    }

    /** Adds a task to kill an elasticsearch node with the given pidfile */
    private TaskProvider<AntFixtureStop> createStopTask() {
        TaskProvider<AntFixtureStop> stop = getProject().getTasks().register(getName() + "#stop", AntFixtureStop.class);
        stop.configure(t -> t.setFixture(this));
        return stop;
    }

    @Internal
    protected File getWrapperScript() {
        return new File(getCwd(), OS.current() == OS.WINDOWS ? "run.bat" : "run");
    }
}
