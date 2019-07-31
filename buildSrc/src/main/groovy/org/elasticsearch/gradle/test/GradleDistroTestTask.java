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

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.vagrant.VagrantShellTask;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.options.Option;

import java.util.Collections;
import java.util.List;

/**
 * Run a gradle task of the current build, within the configured vagrant VM.
 */
public class GradleDistroTestTask extends VagrantShellTask {

    private String taskName;
    private String testClass;

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

    @Override
    protected List<String> getWindowsScript() {
        return getScript("gradlew");
    }

    @Override
    protected List<String> getLinuxScript() {
        return getScript("./gradlew");
    }

    private List<String> getScript(String gradle) {
        StringBuilder line = new StringBuilder();
        line.append(gradle);
        line.append(" ");
        line.append(taskName);
        if (getProject().getGradle().getStartParameter().getLogLevel() == LogLevel.INFO) {
            line.append(" -i");
        }
        if (testClass != null) {
            line.append(" --tests=");
            line.append(testClass);
        }
        return Collections.singletonList(line.toString());
    }
}
