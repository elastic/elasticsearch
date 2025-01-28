/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.OS
import org.elasticsearch.gradle.internal.test.AntFixture
import org.gradle.api.file.FileSystemOperations
import org.gradle.api.file.ProjectLayout
import org.gradle.api.provider.ProviderFactory
import org.gradle.process.ExecOperations

import javax.inject.Inject

abstract class AntFixtureStop extends LoggedExec implements FixtureStop {

    @Inject
    AntFixtureStop(ProjectLayout projectLayout,
                   ExecOperations execOperations,
                   FileSystemOperations fileSystemOperations,
                   ProviderFactory providerFactory) {
        super(projectLayout, execOperations, fileSystemOperations, providerFactory)
    }

    void setFixture(AntFixture fixture) {
        def pidFile = fixture.pidFile
        def fixtureName = fixture.name
        final Object pid = "${-> Integer.parseInt(pidFile.getText('UTF-8').trim())}"
        onlyIf("pidFile exists") { pidFile.exists() }
        doFirst {
            logger.info("Shutting down ${fixtureName} with pid ${pid}")
        }

        if (OS.current() == OS.WINDOWS) {
            getExecutable().set('Taskkill')
            args('/PID', pid, '/F')
        } else {
            getExecutable().set('kill')
            args('-9', pid)
        }
        doLast {
            fileSystemOperations.delete {
                it.delete(pidFile)
            }
        }
    }
}
