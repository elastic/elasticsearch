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

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.FileReader;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class PomValidationTask extends PrecommitTask {

    private final RegularFileProperty pomFile = getProject().getObjects().fileProperty();

    private boolean foundError;

    @InputFile
    public RegularFileProperty getPomFile() {
        return pomFile;
    }

    @TaskAction
    public void checkPom() throws Exception {
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
        if (foundError) {
            throw new GradleException("Check failed for task '" + getPath() + "', see console log for details");
        }
    }

    private void logError(String element, String message) {
        foundError = true;
        getLogger().error("{} {} in [{}]", element, message, pomFile.getAsFile().get());
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
