/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.elasticsearch.gradle.internal.conventions.problems.ElasticsearchBuildProblems;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.Problems;
import org.gradle.api.problems.Severity;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.inject.Inject;

public class PomValidationTask extends PrecommitTask {

    private final RegularFileProperty pomFile;
    private final ProblemReporter problemReporter;
    private List<Problem> collectedProblems;

    @Inject
    public PomValidationTask(ObjectFactory objects, Problems problems) {
        pomFile = objects.fileProperty();
        this.problemReporter = problems.getReporter();
    }

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public RegularFileProperty getPomFile() {
        return pomFile;
    }

    @TaskAction
    public void checkPom() throws Exception {
        collectedProblems = new ArrayList<>();
        try (FileReader fileReader = new FileReader(pomFile.getAsFile().get())) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(fileReader);

            validateString("groupId", model.getGroupId());
            validateString("artifactId", model.getArtifactId());
            validateString("version", model.getVersion());
            validateString("name", model.getName());
            validateString("description", model.getDescription());
            validateString("url", model.getUrl());

            validateCollection("licenses", model.getLicenses(), v -> {
                validateString("licenses.name", v.getName());
                validateString("licenses.url", v.getUrl());
            });

            validateCollection("developers", model.getDevelopers(), v -> {
                validateString("developers.name", v.getName());
                validateString("developers.url", v.getUrl());
            });

            validateNonNull("scm", model.getScm(), () -> validateString("scm.url", model.getScm().getUrl()));
        }
        if (collectedProblems.isEmpty() == false) {
            problemReporter.report(collectedProblems);
            throw new GradleException("Check failed for task '" + getPath() + "', see console log for details");
        }
    }

    private void logError(String element, String message) {
        String pomPath = pomFile.getAsFile().get().getAbsolutePath();
        collectedProblems.add(
            problemReporter.create(
                ProblemId.create("invalid-" + element.replace(".", "-"), "Invalid POM element: " + element, ElasticsearchBuildProblems.POM_VALIDATION),
                spec -> spec.contextualLabel(element + " " + message + " in " + pomPath)
                    .details("POM element '" + element + "' " + message)
                    .severity(Severity.ERROR)
                    .fileLocation(pomPath)
                    .solution("Add a valid '" + element + "' element to the POM file")
            )
        );
    }

    private <T> void validateNonEmpty(String element, T value, Predicate<T> isEmpty) {
        if (isEmpty.test(value)) {
            logError(element, "is empty");
        }
    }

    private <T> void validateNonNull(String element, T value, Runnable validator) {
        if (value == null) {
            logError(element, "is missing");
        } else {
            validator.run();
        }
    }

    private void validateString(String element, String value) {
        validateNonNull(element, value, () -> validateNonEmpty(element, value, String::isBlank));
    }

    private <T> void validateCollection(String element, Collection<T> value, Consumer<T> validator) {
        validateNonNull(element, value, () -> {
            validateNonEmpty(element, value, Collection::isEmpty);
            value.forEach(validator);
        });

    }
}
