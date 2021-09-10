/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.vagrant.VagrantShellTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.options.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertWindowsPath;

/**
 * Run a gradle task of the current build, within the configured vagrant VM.
 */
public class GradleDistroTestTask extends VagrantShellTask {

    private String taskName;
    private String testClass;
    private List<String> extraArgs = new ArrayList<>();

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    @Input
    public String getTaskName() {
        return taskName;
    }

    @Option(option = "tests", description = "Sets test class or method name to be included, '*' is supported.")
    public void setTestClass(String testClass) {
        this.testClass = testClass;
    }

    @Input
    public List<String> getExtraArgs() {
        return extraArgs;
    }

    public void extraArg(String arg) {
        this.extraArgs.add(arg);
    }

    @Override
    protected List<String> getWindowsScript() {
        return getScript(true);
    }

    @Override
    protected List<String> getLinuxScript() {
        return getScript(false);
    }

    private List<String> getScript(boolean isWindows) {
        String cacheDir = getProject().getBuildDir() + "/gradle-cache";
        StringBuilder line = new StringBuilder();
        line.append(isWindows ? "& .\\gradlew " : "./gradlew ");
        line.append(taskName);
        line.append(" --project-cache-dir ");
        line.append(isWindows ? convertWindowsPath(getProject(), cacheDir) : convertLinuxPath(getProject(), cacheDir));
        line.append(" -S");
        line.append(" --parallel");
        line.append(" -D'org.gradle.logging.level'=" + getProject().getGradle().getStartParameter().getLogLevel());
        if (testClass != null) {
            line.append(" --tests=");
            line.append(testClass);
        }
        extraArgs.stream().map(s -> " " + s).forEach(line::append);
        return Collections.singletonList(line.toString());
    }
}
