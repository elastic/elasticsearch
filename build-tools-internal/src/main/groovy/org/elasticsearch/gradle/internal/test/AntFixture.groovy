/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.OS
import groovy.ant.AntBuilder
import org.elasticsearch.gradle.internal.AntFixtureStop
import org.elasticsearch.gradle.internal.AntTask
import org.elasticsearch.gradle.testclusters.TestClusterInfo
import org.elasticsearch.gradle.testclusters.TestClusterValueSource
import org.elasticsearch.gradle.testclusters.TestClustersRegistry
import org.gradle.api.GradleException
import org.gradle.api.file.ProjectLayout
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ProviderFactory
import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskProvider
import javax.inject.Inject

/**
 * A fixture for integration tests which runs in a separate process launched by Ant.
 */
class AntFixture extends AntTask {

    /** The path to the executable that starts the fixture. */
    @Internal
    String executable

    private final List<Object> arguments = new ArrayList<>()
    private ProjectLayout projectLayout
    private final ProviderFactory providerFactory

    void args(Object... args) {
        arguments.addAll(args)
    }

    /**
     * Environment variables for the fixture process. The value can be any object, which
     * will have toString() called at execution time.
     */
    private final Map<String, Object> environment = new HashMap<>()

    void env(String key, Object value) {
        environment.put(key, value)
    }

    /** A flag to indicate whether the command should be executed from a shell. */
    @Internal
    boolean useShell = false

    @Internal
    int maxWaitInSeconds = 30

    /**
     * A flag to indicate whether the fixture should be run in the foreground, or spawned.
     * It is protected so subclasses can override (eg RunTask).
     */
    protected boolean spawn = true

    /**
     * A closure to call before the fixture is considered ready. The closure is passed the fixture object,
     * as well as a groovy AntBuilder, to enable running ant condition checks. The default wait
     * condition is for http on the http port.
     */
    @Internal
    Closure waitCondition = { AntFixture fixture, AntBuilder ant ->
        File tmpFile = new File(fixture.cwd, 'wait.success')
        ant.get(src: "http://${fixture.addressAndPort}",
                dest: tmpFile.toString(),
                ignoreerrors: true, // do not fail on error, so logging information can be flushed
                retries: 10)
        return tmpFile.exists()
    }

    @Inject
    AntFixture(ProjectLayout projectLayout, ProviderFactory providerFactory) {
        this.providerFactory = providerFactory
        this.projectLayout = projectLayout;
        TaskProvider<AntFixtureStop> stopTask = createStopTask()
        finalizedBy(stopTask)
    }

    @Override
    protected void runAnt(AntBuilder ant) {
        // reset everything
        getFileSystemOperations().delete {
            it.delete(baseDir)
        }
        cwd.mkdirs()
        final String realExecutable
        final List<Object> realArgs = new ArrayList<>()
        final Map<String, Object> realEnv = environment
        // We need to choose which executable we are using. In shell mode, or when we
        // are spawning and thus using the wrapper script, the executable is the shell.
        if (useShell || spawn) {
            if (OS.current() == OS.WINDOWS) {
                realExecutable = 'cmd'
                realArgs.add('/C')
                realArgs.add('"') // quote the entire command
            } else {
                realExecutable = 'sh'
            }
        } else {
            realExecutable = executable
            realArgs.addAll(arguments)
        }
        if (spawn) {
            writeWrapperScript(executable)
            realArgs.add(wrapperScript)
            realArgs.addAll(arguments)
        }
        if (OS.current() == OS.WINDOWS && (useShell || spawn)) {
            realArgs.add('"')
        }
        commandString.eachLine { line -> logger.info(line) }

        ant.exec(executable: realExecutable, spawn: spawn, dir: cwd, taskname: name) {
            realEnv.each { key, value -> env(key: key, value: value) }
            realArgs.each { arg(value: it) }
        }

        String failedProp = "failed${name}"
        // first wait for resources, or the failure marker from the wrapper script
        ant.waitfor(maxwait: maxWaitInSeconds, maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: failedProp) {
            or {
                resourceexists {
                    file(file: failureMarker.toString())
                }
                and {
                    resourceexists {
                        file(file: pidFile.relativePath(baseDir).toString())
                    }
                    resourceexists {
                        file(file: portsFile.toString())
                    }
                }
            }
        }

        if (ant.project.getProperty(failedProp) || failureMarker.exists()) {
            fail("Failed to start ${name}")
        }

        // the process is started (has a pid) and is bound to a network interface
        // so now evaluates if the waitCondition is successful
        // TODO: change this to a loop?
        boolean success
        try {
            success = waitCondition(this, ant)
        } catch (Exception e) {
            String msg = "Wait condition caught exception for ${name}"
            logger.error(msg, e)
            fail(msg, e)
        }
        if (success == false) {
            fail("Wait condition failed for ${name}")
        }
    }

    /** Returns a debug string used to log information about how the fixture was run. */
    @Internal
    protected String getCommandString() {
        String commandString = "\n${name} configuration:\n"
        commandString += "-----------------------------------------\n"
        commandString += "  cwd: ${cwd}\n"
        commandString += "  command: ${executable} ${arguments.join(' ')}\n"
        commandString += '  environment:\n'
        environment.each { k, v -> commandString += "    ${k}: ${v}\n" }
        if (spawn) {
            commandString += "\n  [${wrapperScript.name}]\n"
            wrapperScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
        }
        return commandString
    }

    /**
     * Writes a script to run the real executable, so that stdout/stderr can be captured.
     * TODO: this could be removed if we do use our own ProcessBuilder and pump output from the process
     */
    private void writeWrapperScript(String executable) {
        wrapperScript.parentFile.mkdirs()
        String argsPasser = '"$@"'
        String exitMarker = "; if [ \$? != 0 ]; then touch run.failed; fi"
        if (OS.current() == OS.WINDOWS) {
            argsPasser = '%*'
            exitMarker = "\r\n if \"%errorlevel%\" neq \"0\" ( type nul >> run.failed )"
        }
        wrapperScript.setText("\"${executable}\" ${argsPasser} > run.log 2>&1 ${exitMarker}", 'UTF-8')
    }

    /** Fail the build with the given message, and logging relevant info*/
    private void fail(String msg, Exception... suppressed) {
        if (logger.isInfoEnabled() == false) {
            // We already log the command at info level. No need to do it twice.
            commandString.eachLine { line -> logger.error(line) }
        }
        logger.error("${name} output:")
        logger.error("-----------------------------------------")
        logger.error("  failure marker exists: ${failureMarker.exists()}")
        logger.error("  pid file exists: ${pidFile.exists()}")
        logger.error("  ports file exists: ${portsFile.exists()}")
        // also dump the log file for the startup script (which will include ES logging output to stdout)
        if (runLog.exists()) {
            logger.error("\n  [log]")
            runLog.eachLine { line -> logger.error("    ${line}") }
        }
        logger.error("-----------------------------------------")
        GradleException toThrow = new GradleException(msg)
        for (Exception e : suppressed) {
            toThrow.addSuppressed(e)
        }
        throw toThrow
    }

    /** Adds a task to kill an elasticsearch node with the given pidfile */
    private TaskProvider<AntFixtureStop> createStopTask() {
        final AntFixture fixture = this
        TaskProvider<AntFixtureStop> stop = project.tasks.register("${name}#stop", AntFixtureStop)
        stop.configure {
            it.fixture = fixture
        }
        fixture.finalizedBy(stop)
        return stop
    }

    /**
     * A path relative to the build dir that all configuration and runtime files
     * will live in for this fixture
     */
    @Internal
    protected File getBaseDir() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "fixtures/${name}")
    }

    /** Returns the working directory for the process. Defaults to "cwd" inside baseDir. */
    @Internal
    protected File getCwd() {
        return new File(baseDir, 'cwd')
    }

    /** Returns the file the process writes its pid to. Defaults to "pid" inside baseDir. */
    @Internal
    File getPidFile() {
        return new File(baseDir, 'pid')
    }

    /** Reads the pid file and returns the process' pid */
    @Internal
    int getPid() {
        return Integer.parseInt(pidFile.getText('UTF-8').trim())
    }

    /** Returns the file the process writes its bound ports to. Defaults to "ports" inside baseDir. */
    @Internal
    protected File getPortsFile() {
        return new File(baseDir, 'ports')
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    @Internal
    String getAddressAndPort() {
        return portsFile.readLines("UTF-8").get(0)
    }

    @Internal
    Provider<String> getAddressAndPortProvider() {
        File thePortFile = portsFile
        return providerFactory.provider(() -> thePortFile.readLines("UTF-8").get(0))
    }

    /** Returns a file that wraps around the actual command when {@code spawn == true}. */
    @Internal
    protected File getWrapperScript() {
        return new File(cwd, (OS.current() == OS.WINDOWS) ? 'run.bat' : 'run')
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getFailureMarker() {
        return new File(cwd, 'run.failed')
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getRunLog() {
        return new File(cwd, 'run.log')
    }

    @Internal
    Provider<AntFixtureValueSource> getAddressAndPortSource() {
        return providerFactory.of(AntFixtureValueSource.class, spec -> {
            spec.getParameters().getPortFile().set(portsFile);
        });
    }

    static abstract class AntFixtureValueSource implements ValueSource<String, AntFixtureValueSource.Parameters> {
        @Override
        String obtain() {
            return getParameters().getPortFile().map { it.readLines("UTF-8").get(0) }.get()
        }

        interface Parameters extends ValueSourceParameters {
            Property<File> getPortFile();
        }
    }
}
