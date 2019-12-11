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
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BatsTestTask extends DefaultTask {

    private final DirectoryProperty testsDir;
    private final DirectoryProperty utilsDir;
    private final DirectoryProperty distributionsDir;
    private final DirectoryProperty pluginsDir;
    private final DirectoryProperty upgradeDir;
    private String packageName;

    public BatsTestTask() {
        this.testsDir = getProject().getObjects().directoryProperty();
        this.utilsDir = getProject().getObjects().directoryProperty();
        this.distributionsDir = getProject().getObjects().directoryProperty();
        this.pluginsDir = getProject().getObjects().directoryProperty();
        this.upgradeDir = getProject().getObjects().directoryProperty();
    }

    @InputDirectory
    public Provider<Directory> getTestsDir() {
        return testsDir;
    }

    public void setTestsDir(Directory testsDir) {
        this.testsDir.set(testsDir);
    }

    @InputDirectory
    public Provider<Directory> getUtilsDir() {
        return utilsDir;
    }

    public void setUtilsDir(Directory utilsDir) {
        this.utilsDir.set(utilsDir);
    }

    @InputDirectory
    public Provider<Directory> getDistributionsDir() {
        return distributionsDir;
    }

    public void setDistributionsDir(Provider<Directory> distributionsDir) {
        this.distributionsDir.set(distributionsDir);
    }

    @InputDirectory
    @Optional
    public Provider<Directory> getPluginsDir() {
        return pluginsDir;
    }

    public void setPluginsDir(Provider<Directory> pluginsDir) {
        this.pluginsDir.set(pluginsDir);
    }

    @InputDirectory
    @Optional
    public Provider<Directory> getUpgradeDir() {
        return upgradeDir;
    }

    public void setUpgradeDir(Provider<Directory> upgradeDir) {
        this.upgradeDir.set(upgradeDir);
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
            spec.environment("BATS_TESTS", testsDir.getAsFile().get().toString());
            spec.environment("BATS_UTILS", utilsDir.getAsFile().get().toString());
            if (pluginsDir.isPresent()) {
                spec.environment("BATS_PLUGINS", pluginsDir.getAsFile().get().toString());
            }
            if (upgradeDir.isPresent()) {
                spec.environment("BATS_UPGRADE", upgradeDir.getAsFile().get().toString());
            }
            spec.environment("PACKAGE_NAME", packageName);
            spec.setCommandLine(command);
        });
    }
}
