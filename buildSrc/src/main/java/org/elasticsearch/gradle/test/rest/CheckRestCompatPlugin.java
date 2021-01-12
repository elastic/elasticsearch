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

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.internal.InternalPlugin;
import org.gradle.api.Project;

/**
 * Lifecycle plugin to anchor all Rest compatibility testing to a single command. Usage: `checkRestCompat`
 */
public class CheckRestCompatPlugin implements InternalPlugin {

    public static final String CHECK_TASK_NAME = "checkRestCompat";

    @Override
    public void apply(Project project) {
        project.getTasks().register("checkRestCompat", (checkTask) -> {
            checkTask.setDescription("Runs all REST compatibility checks.");
            checkTask.setGroup("verification");
        });
    }
}
