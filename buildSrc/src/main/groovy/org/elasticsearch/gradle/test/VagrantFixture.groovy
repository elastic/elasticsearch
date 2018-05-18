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

    private VagrantCommandTask stopTask

    public VagrantFixture() {
        this.stopTask = project.tasks.create(name: "${name}#stop", type: VagrantCommandTask) {
            command 'halt'
        }
        finalizedBy this.stopTask
    }

    @Override
    void setBoxName(String boxName) {
        super.setBoxName(boxName)
        this.stopTask.setBoxName(boxName)
    }

    @Override
    void setEnvironmentVars(Map<String, String> environmentVars) {
        super.setEnvironmentVars(environmentVars)
        this.stopTask.setEnvironmentVars(environmentVars)
    }

    @Override
    public Task getStopTask() {
        return this.stopTask
    }
}
