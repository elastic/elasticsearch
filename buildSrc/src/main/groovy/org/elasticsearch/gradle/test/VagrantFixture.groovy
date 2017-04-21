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
import org.gradle.api.Task

/**
 * A fixture for integration tests which runs in a virtual machine launched by Vagrant.
 */
class VagrantFixture extends VagrantCommandTask implements Fixture {

    private Task stopTask = null

    public VagrantFixture() {
        project.afterEvaluate {
            // force the initialization of the stop task at the end
            // of the project if it hasn't been yet.
            getStopTask()
        }
    }

    private Task createStopTask() {
        VagrantCommandTask halt = project.tasks.create(name: "${name}#stop", type: VagrantCommandTask) {
            args 'halt', this.boxName
        }
        halt.boxName = this.boxName
        halt.environmentVars = this.environmentVars
        return halt;
    }

    @Override
    public Task getStopTask() {
        // Lazy init the stop task since creating it in the constructor
        // means that it captures variable values that may not be set in
        // the task yet.
        if (stopTask == null) {
            stopTask = createStopTask()
            finalizedBy(stopTask)
        }
        return stopTask
    }
}
