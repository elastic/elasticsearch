/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.util.Util;
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
                // This must always be specified precisely, so that
                // projects other than `rest-api-spec` can use this task.
                task.setJsonSchema(new File(project.getRootDir(), "rest-api-spec/src/main/resources/schema.json"));
                task.setReport(new File(project.getBuildDir(), "reports/validateJson.txt"));
            });

        Provider<ValidateJsonNoKeywordsTask> validateNoKeywordsTask = project.getTasks()
            .register("validateNoKeywords", ValidateJsonNoKeywordsTask.class, task -> {
                task.setInputFiles(Util.getJavaTestAndMainSourceResources(project, filter -> {
                    filter.include(DOUBLE_STAR + "/rest-api-spec/api/" + DOUBLE_STAR + "/*.json");
                    filter.exclude(DOUBLE_STAR + "/_common.json");
                }));
                task.setJsonKeywords(new File(project.getRootDir(), "rest-api-spec/keywords.json"));
                task.setReport(new File(project.getBuildDir(), "reports/validateKeywords.txt"));
                // There's no point running this task if the schema validation fails
                task.mustRunAfter(validateRestSpecTask);
            });

        project.getTasks().named("precommit").configure(t -> t.dependsOn(validateRestSpecTask, validateNoKeywordsTask));
    }
}
