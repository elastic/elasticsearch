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

import org.apache.commons.io.output.TeeOutputStream
import org.elasticsearch.gradle.LoggedExec
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction
import org.gradle.internal.logging.progress.ProgressLoggerFactory

import javax.inject.Inject
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantLock

/**
 * Runs a vagrant command. Pretty much like Exec task but with a nicer output
 * formatter and defaults to `vagrant` as first part of commandLine.
 */
public class VagrantCommandTask extends LoggedExec {

    @Input
    String command

    @Input @Optional
    String subcommand

    @Input
    String boxName

    @Input
    Map<String, String> environmentVars

    public VagrantCommandTask() {
        executable = 'vagrant'

        // We're using afterEvaluate here to slot in some logic that captures configurations and
        // modifies the command line right before the main execution happens. The reason that we
        // call doFirst instead of just doing the work in the afterEvaluate is that the latter
        // restricts how subclasses can extend functionality. Calling afterEvaluate is like having
        // all the logic of a task happening at construction time, instead of at execution time
        // where a subclass can override or extend the logic.
        project.afterEvaluate {
            doFirst {
                if (environmentVars != null) {
                    environment environmentVars
                }

                // Build our command line for vagrant
                def vagrantCommand = [executable, command]
                if (subcommand != null) {
                    vagrantCommand = vagrantCommand + subcommand
                }
                commandLine([*vagrantCommand, boxName, *args])

                // It'd be nice if --machine-readable were, well, nice
                standardOutput = new TeeOutputStream(standardOutput, createLoggerOutputStream())
            }
        }
    }

    @Inject
    ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException()
    }

    protected OutputStream createLoggerOutputStream() {
        return new VagrantLoggerOutputStream(
            command: commandLine.join(' '),
            factory: getProgressLoggerFactory(),
            /* Vagrant tends to output a lot of stuff, but most of the important
              stuff starts with ==> $box */
            squashedPrefix: "==> $boxName: ")
    }
}
