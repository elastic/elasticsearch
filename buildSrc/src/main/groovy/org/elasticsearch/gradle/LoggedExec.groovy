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

package org.elasticsearch.gradle

import org.gradle.api.GradleException
import org.gradle.api.tasks.Exec

/**
 * A wrapper around gradle's Exec task to capture output and log on error.
 */
class LoggedExec extends Exec {

    protected ByteArrayOutputStream output = new ByteArrayOutputStream()

    LoggedExec() {
        if (logger.isInfoEnabled() == false) {
            standardOutput = output
            errorOutput = output
            ignoreExitValue = true
            doLast {
                if (execResult.exitValue != 0) {
                    output.toString('UTF-8').eachLine { line -> logger.error(line) }
                    throw new GradleException("Process '${executable} ${args.join(' ')}' finished with non-zero exit value ${execResult.exitValue}")
                }
            }
        }
    }
}
