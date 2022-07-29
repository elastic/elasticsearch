/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.internal.test.AntFixture
import org.gradle.api.file.FileSystemOperations
import org.gradle.api.file.ProjectLayout
import org.gradle.api.tasks.Internal
import org.gradle.process.ExecOperations

import javax.inject.Inject

abstract class AntFixtureStop extends LoggedExec implements FixtureStop {

    @Internal
    AntFixture fixture

    @Inject
    AntFixtureStop(ProjectLayout projectLayout, ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
       super(projectLayout, execOperations, fileSystemOperations)
    }

    void setFixture(AntFixture fixture) {
        assert this.fixture == null
        this.fixture = fixture;
        final Object pid = "${ -> this.fixture.pid }"
        onlyIf { fixture.pidFile.exists() }
        doFirst {
            logger.info("Shutting down ${fixture.name} with pid ${pid}")
        }

        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            getExecutable().set('Taskkill')
            args('/PID', pid, '/F')
        } else {
            getExecutable().set('kill')
            args('-9', pid)
        }
        doLast {
            fileSystemOperations.delete {
                it.delete(fixture.pidFile)
            }
        }
        this.fixture = fixture
    }
}
