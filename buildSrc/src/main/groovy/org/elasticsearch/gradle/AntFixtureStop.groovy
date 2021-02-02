/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.test.AntFixture
import org.gradle.api.file.FileSystemOperations
import org.gradle.api.tasks.Internal

import javax.inject.Inject

class AntFixtureStop extends LoggedExec implements FixtureStop {

    @Internal
    AntFixture fixture

    @Internal
    FileSystemOperations fileSystemOperations

    @Inject
    AntFixtureStop(FileSystemOperations fileSystemOperations) {
        super(fileSystemOperations)
        this.fileSystemOperations = fileSystemOperations
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
            executable = 'Taskkill'
            args('/PID', pid, '/F')
        } else {
            executable = 'kill'
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
