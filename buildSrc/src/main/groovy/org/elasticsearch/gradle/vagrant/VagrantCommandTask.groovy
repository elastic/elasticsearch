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
import org.elasticsearch.gradle.ProgressLoggerFactoryInjection
import org.elasticsearch.gradle.LoggedExec
import org.gradle.api.tasks.Input

/**
 * Runs a vagrant command. Pretty much like Exec task but with a nicer output
 * formatter and defaults to `vagrant` as first part of commandLine.
 */
public class VagrantCommandTask extends LoggedExec implements ProgressLoggerFactoryInjection {

    @Input
    String boxName

    @Input
    Map<String, String> environmentVars

    public VagrantCommandTask() {
        executable = 'vagrant'

        project.afterEvaluate {
            // It'd be nice if --machine-readable were, well, nice
            standardOutput = new TeeOutputStream(standardOutput, createLoggerOutputStream())
            if (environmentVars != null) {
                environment environmentVars
            }
        }
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
