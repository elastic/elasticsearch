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

package org.elasticsearch.gradle.precommit;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;

public class TestingConventionsPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<TestingConventionsTasks> testingConventions = project.getTasks()
            .register("testingConventions", TestingConventionsTasks.class);
        testingConventions.configure(t -> {
            TestingConventionRule testsRule = t.getNaming().maybeCreate("Tests");
            testsRule.baseClass("org.apache.lucene.util.LuceneTestCase");
            TestingConventionRule itRule = t.getNaming().maybeCreate("IT");
            itRule.baseClass("org.elasticsearch.test.ESIntegTestCase");
            itRule.baseClass("org.elasticsearch.test.rest.ESRestTestCase");
        });
        return testingConventions;
    }
}
