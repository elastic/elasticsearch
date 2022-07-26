/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.Callable;

public abstract class LoggedExecFixture extends LoggedExec {

    @Inject
    public LoggedExecFixture(ProjectLayout projectLayout, ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
        super(projectLayout, execOperations, fileSystemOperations);
        getWaitingCondition().convention(() -> {
            URL url = new URL("http://" + getAddressAndPort());
            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(url.openStream()));
                while ((in.readLine()) != null)
                    in.close();
            } catch(Exception e) {
              return false;
            }
            return true;
        });
    }

    /**
     * A path relative to the build dir that all configuration and runtime files
     * will live in for this fixture
     */
    @Internal
    protected File getBaseDir() {
        return projectLayout.getBuildDirectory().dir("fixtures/" + getName()).get().getAsFile()
    }

    @Input
    abstract public Property<Callable<Boolean>> getWaitingCondition();

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
    int getPid() throws IOException {
        return Integer.parseInt(FileUtils.readFileToString(getPidFile()).trim());
    }

    /** Returns the file the process writes its bound ports to. Defaults to "ports" inside baseDir. */
    @Internal
    protected File getPortsFile() {
        return new File(getBaseDir(), "ports");
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    @Internal
    String getAddressAndPort() throws IOException {
        return FileUtils.readLines(getPortsFile(), "UTF-8").get(0);
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
}
