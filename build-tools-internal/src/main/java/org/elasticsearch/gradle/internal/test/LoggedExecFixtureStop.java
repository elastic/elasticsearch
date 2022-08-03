/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.internal.FixtureStop;
import org.gradle.api.Action;
import org.gradle.api.Task;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.Internal;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;

public abstract class LoggedExecFixtureStop extends LoggedExec implements FixtureStop {

    @Internal
    private LoggedExecFixture fixture;

    public LoggedExecFixture getFixture() {
        return fixture;
    }

    @Inject
    public LoggedExecFixtureStop(ProjectLayout projectLayout, ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
        super(projectLayout, execOperations, fileSystemOperations);
    }

    public void setFixture(final LoggedExecFixture fixture) {
        assert this.fixture == null;
        this.fixture = fixture;
        onlyIf(task -> fixture.getPidFile().exists());
        doFirst(new Action<Task>() {
            @Override
            public void execute(Task task) {
                getLogger().info("Shutting down " + fixture.getName() + " with pid " + fixture.getPid());

            }
        });
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            getExecutable().set("Taskkill");
            args("/PID");
            getArgs().add(getProject().provider(() -> fixture.getPid()));
            args("/F");
        } else {
            getExecutable().set("kill");
            getArgs().add("-9");
            getArgs().add(getProject().provider(() -> fixture.getPid()));
        }
        doLast(new Action<Task>() {
            @Override
            public void execute(Task task) {
                fileSystemOperations.delete(spec -> spec.delete(fixture.getPidFile()));
            }
        });
        this.fixture = fixture;
    }

}
