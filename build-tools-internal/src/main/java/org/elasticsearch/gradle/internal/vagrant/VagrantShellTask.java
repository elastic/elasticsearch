/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.vagrant;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.initialization.layout.BuildLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertWindowsPath;

/**
 * A shell script to run within a vagrant VM.
 *
 * The script is run as root within the VM.
 */
public abstract class VagrantShellTask extends DefaultTask {

    private VagrantExtension extension;
    private VagrantMachine service;

    private UnaryOperator<String> progressHandler = UnaryOperator.identity();
    private BuildLayout buildLayout;

    public VagrantShellTask(BuildLayout buildLayout) {
        this.buildLayout = buildLayout;
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

    public void setExtension(VagrantExtension extension) {
        this.extension = extension;
    }

    public void setService(VagrantMachine service) {
        this.service = service;
    }

    @TaskAction
    public void runScript() {
        if (extension.isWindowsVM()) {
            service.execute(spec -> {
                spec.setCommand("winrm");
                List<String> script = new ArrayList<>();
                script.add("try {");
                script.add("cd " + convertWindowsPath(buildLayout.getRootDirectory(), buildLayout.getRootDirectory().toString()));
                extension.getVmEnv().forEach((k, v) -> script.add("$Env:" + k + " = \"" + v + "\""));
                script.addAll(getWindowsScript().stream().map(s -> "    " + s).toList());
                script.addAll(
                    Arrays.asList(
                        "    exit $LASTEXITCODE",
                        "} catch {",
                        // catch if we have a failure to even run the script at all above, equivalent to set -e, sort of
                        "    echo $_.Exception.Message",
                        "    exit 1",
                        "}"
                    )
                );
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
                    script.add("cd " + convertLinuxPath(buildLayout.getRootDirectory(), buildLayout.getRootDirectory().toString()));
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
