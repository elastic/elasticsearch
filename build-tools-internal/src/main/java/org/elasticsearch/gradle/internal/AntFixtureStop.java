/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.internal.test.AntFixture;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public abstract class AntFixtureStop extends LoggedExec implements FixtureStop {

    @Inject
    public AntFixtureStop(
        ProjectLayout projectLayout,
        ExecOperations execOperations,
        FileSystemOperations fileSystemOperations,
        ProviderFactory providerFactory
    ) {
        super(projectLayout, execOperations, fileSystemOperations, providerFactory);
    }

    public void setFixture(AntFixture fixture) {
        File pidFile = fixture.getPidFile();
        String fixtureName = fixture.getName();

        // Corrección de onlyIf: Usamos la sintaxis estándar de Task
        this.onlyIf("pidFile exists", task -> pidFile.exists());

        doFirst(t -> {
            try {
                // Leemos el PID usando Java puro
                String pid = Files.readString(pidFile.toPath(), StandardCharsets.UTF_8).trim();
                getLogger().info("Shutting down " + fixtureName + " with pid " + pid);

                if (OS.current() == OS.WINDOWS) {
                    this.getExecutable().set("taskkill");
                    args("/PID", pid, "/F");
                } else {
                    this.getExecutable().set("kill");
                    args("-9", pid);
                }
            } catch (IOException e) {
                getLogger().error("Failed to read PID file", e);
            }
        });

        doLast(t -> { getProject().delete(pidFile); });
    }
}
