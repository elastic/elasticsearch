/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.elasticsearch.gradle.internal.conventions.GUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.TaskProvider;

/**
 * Adds pom validation to every pom generation task.
 */
public class PomValidationPrecommitPlugin extends PrecommitPlugin {

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<Task> validatePom = project.getTasks().register("validatePom");
        PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);
        publishing.getPublications().configureEach(publication -> {
            String publicationName = GUtils.capitalize(publication.getName());
            TaskProvider<PomValidationTask> validateTask = project.getTasks()
                .register("validate" + publicationName + "Pom", PomValidationTask.class);
            validatePom.configure(t -> t.dependsOn(validateTask));
            validateTask.configure(task -> {
                GenerateMavenPom generateMavenPom = project.getTasks()
                    .withType(GenerateMavenPom.class)
                    .getByName("generatePomFileFor" + publicationName + "Publication");
                task.dependsOn(generateMavenPom);
                task.getPomFile().fileValue(generateMavenPom.getDestination());
            });
        });

        return validatePom;
    }
}
