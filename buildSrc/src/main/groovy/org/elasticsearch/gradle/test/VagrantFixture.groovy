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
package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.vagrant.VagrantCommandTask

/**
 * A fixture for integration tests which runs in a virtual machine launched by Vagrant.
 */
class VagrantFixture extends VagrantCommandTask implements Fixture {

    public VagrantFixture() {
        // Our stop task is a VagrandCommandTask that halts the VM that we supposedly just stood up.
        // VagrantCommandTask schedules an afterEvaluate closure that sets the env variables for the vagrant
        // command to use. We want the following closure to run before that does, because the following closure
        // configures the stop task with the same environment variables and box name from the start command on
        // the fixture. If this does not run first, then the stop task will capture an empty set of environment
        // variables at the end of the project set up instead of the environment variables of the start command.
        project.afterEvaluate {
            def startCommandBoxName = this.boxName
            def startCommandEnvVariables = this.environmentVars
            VagrantCommandTask halt = project.tasks.getByName("${name}#stop") {
                boxName startCommandBoxName
                environmentVars startCommandEnvVariables
                args 'halt', startCommandBoxName
            }
            finalizedBy(halt)
        }
        // Now that is scheduled, create the stop command. We'll configure it after the project is
        // done evaluating with the above closure so that it picks up any new configurations to the
        // Fixture after the constructor is done running (like the box name and the environment variables)
        project.tasks.create(name: "${name}#stop", type: VagrantCommandTask)
    }

    @Override
    public String getStopTask() {
        return "${name}#stop"
    }
}
