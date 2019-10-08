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

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertWindowsPath;

/**
 * A shell script to run within a vagrant VM.
 *
 * The script is run as root within the VM.
 */
public abstract class VagrantShellTask extends DefaultTask {

    private final VagrantExtension extension;
    private final VagrantMachine service;
    private UnaryOperator<String> progressHandler = UnaryOperator.identity();

    public VagrantShellTask() {
        extension = getProject().getExtensions().findByType(VagrantExtension.class);
        if (extension == null) {
            throw new IllegalStateException("elasticsearch.vagrant-base must be applied to create " + getClass().getName());
        }
        service = getProject().getExtensions().getByType(VagrantMachine.class);
    }

    @Input
    protected abstract List<String> getWindowsScript();

    @Input
    protected abstract List<String> getLinuxScript();

    @Input
    public UnaryOperator<String> getProgressHandler() {
        return progressHandler;
    }

    public void setProgressHandler(UnaryOperator<String> progressHandler) {
        this.progressHandler = progressHandler;
    }

    @TaskAction
    public void runScript() {
        String rootDir = getProject().getRootDir().toString();
        if (extension.isWindowsVM()) {
            service.execute(spec -> {
                spec.setCommand("winrm");

                List<String> script = new ArrayList<>();
                script.add("try {");
                script.add("cd " + convertWindowsPath(getProject(), rootDir));
                extension.getVmEnv().forEach((k, v) -> script.add("$Env:" + k + " = \"" + v + "\""));
                script.addAll(getWindowsScript().stream().map(s -> "    " + s).collect(Collectors.toList()));
                script.addAll(Arrays.asList(
                    "    exit $LASTEXITCODE",
                    "} catch {",
                    // catch if we have a failure to even run the script at all above, equivalent to set -e, sort of
                    "    echo $_.Exception.Message",
                    "    exit 1",
                    "}"));
                spec.setArgs("--elevated", "--command", String.join("\n", script));
                spec.setProgressHandler(progressHandler);
            });
        } else {
            try {
                service.execute(spec -> {
                    spec.setCommand("ssh");

                    List<String> script = new ArrayList<>();
                    script.add("sudo bash -c '"); // start inline bash script
                    script.add("pwd");
                    script.add("cd " + convertLinuxPath(getProject(), rootDir));
                    extension.getVmEnv().forEach((k, v) -> script.add("export " + k + "=" + v));
                    script.addAll(getLinuxScript());
                    script.add("'"); // end inline bash script
                    spec.setArgs("--command", String.join("\n", script));
                    spec.setProgressHandler(progressHandler);
                });
            } catch (Exception e) {
                /*getLogger().error("Failed command, dumping dmesg", e);
                service.execute(spec -> {
                    spec.setCommand("ssh");
                    spec.setArgs("--command", "dmesg");
                    spec.setProgressHandler(line -> {
                        getLogger().error(line);
                        return null;
                    });
                });*/
                throw e;
            }
        }
    }


}
