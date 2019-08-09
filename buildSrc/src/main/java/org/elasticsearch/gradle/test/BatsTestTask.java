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

import org.gradle.api.DefaultTask;
import org.gradle.api.file.Directory;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BatsTestTask extends DefaultTask {

    private Directory testsDir;
    private Directory utilsDir;
    private Directory distributionsDir;
    private Directory pluginsDir;
    private Directory upgradeDir;

    private String packageName;

    @InputDirectory
    public Directory getTestsDir() {
        return testsDir;
    }

    public void setTestsDir(Directory testsDir) {
        this.testsDir = testsDir;
    }

    @InputDirectory
    public Directory getUtilsDir() {
        return utilsDir;
    }

    public void setUtilsDir(Directory utilsDir) {
        this.utilsDir = utilsDir;
    }

    @InputDirectory
    public Directory getDistributionsDir() {
        return distributionsDir;
    }

    public void setDistributionsDir(Directory distributionsDir) {
        this.distributionsDir = distributionsDir;
    }

    @InputDirectory
    @Optional
    public Directory getPluginsDir() {
        return pluginsDir;
    }

    public void setPluginsDir(Directory pluginsDir) {
        this.pluginsDir = pluginsDir;
    }

    @InputDirectory
    @Optional
    public Directory getUpgradeDir() {
        return upgradeDir;
    }

    public void setUpgradeDir(Directory upgradeDir) {
        this.upgradeDir = upgradeDir;
    }

    @Input
    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    @TaskAction
    public void runBats() {
        List<Object> command = new ArrayList<>();
        command.add("bats");
        command.add("--tap");
        command.addAll(testsDir.getAsFileTree().getFiles().stream()
            .filter(f -> f.getName().endsWith(".bats"))
            .sorted().collect(Collectors.toList()));
        getProject().exec(spec -> {
            spec.setWorkingDir(distributionsDir.getAsFile());
            spec.environment(System.getenv());
            spec.environment("BATS_TESTS", testsDir.getAsFile().toString());
            spec.environment("BATS_UTILS", utilsDir.getAsFile().toString());
            if (pluginsDir != null) {
                spec.environment("BATS_PLUGINS", pluginsDir.getAsFile().toString());
            }
            if (upgradeDir != null) {
                spec.environment("BATS_UPGRADE", upgradeDir.getAsFile().toString());
            }
            spec.environment("PACKAGE_NAME", packageName);
            spec.setCommandLine(command);
        });
    }
}
