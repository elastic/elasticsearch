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

import org.elasticsearch.gradle.util.Util;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

import java.io.File;

public class ValidateRestSpecPlugin implements Plugin<Project> {
    private static final String DOUBLE_STAR = "**"; // checkstyle thinks these are javadocs :(

    @Override
    public void apply(Project project) {
        Provider<ValidateJsonAgainstSchemaTask> validateRestSpecTask = project.getTasks()
            .register("validateRestSpec", ValidateJsonAgainstSchemaTask.class, task -> {
                task.setInputFiles(Util.getJavaTestAndMainSourceResources(project, filter -> {
                    filter.include(DOUBLE_STAR + "/rest-api-spec/api/" + DOUBLE_STAR + "/*.json");
                    filter.exclude(DOUBLE_STAR + "/_common.json");
                }));
                task.setJsonSchema(new File(project.getRootDir(), "rest-api-spec/src/main/resources/schema.json"));
            });
        project.getTasks().named("precommit").configure(t -> t.dependsOn(validateRestSpecTask));
    }
}
