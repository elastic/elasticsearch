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
package org.elasticsearch.gradle.vagrant

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.logging.ProgressLoggerFactory
import org.gradle.process.internal.ExecAction
import org.gradle.process.internal.ExecActionFactory

import javax.inject.Inject

/**
 * Runs bats over vagrant. Pretty much like running it using Exec but with a
 * nicer output formatter.
 */
public class BatsOverVagrantTask extends VagrantCommandTask {

    @Input
    String command

    BatsOverVagrantTask() {
        project.afterEvaluate {
            args 'ssh', boxName, '--command', command
        }
    }

    @Override
    protected OutputStream createLoggerOutputStream() {
        return new TapLoggerOutputStream(
                command: commandLine.join(' '),
                factory: getProgressLoggerFactory(),
                logger: logger)
    }
}
