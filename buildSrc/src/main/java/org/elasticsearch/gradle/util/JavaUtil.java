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

package org.elasticsearch.gradle.util;

import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.JavaHome;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskExecutionGraph;
import org.gradle.api.plugins.ExtraPropertiesExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JavaUtil {

    /** A convenience method for getting java home for a version of java and requiring that version for the given task to execute */
    static String getJavaHome(final Task task, final int version) {
        List<JavaHome> javaHomes = BuildParams.getJavaVersions();
        Optional<JavaHome> java = javaHomes.stream().filter(j -> j.getVersion() == version).findFirst();
        // check directly if the version is present since we are already executing
        if (java.isEmpty()) {
            throw new GradleException("JAVA" + version + "_HOME required to run task " + task.getPath());
        }
        return java.get().getJavaHome().get().getAbsolutePath();
    }
}
