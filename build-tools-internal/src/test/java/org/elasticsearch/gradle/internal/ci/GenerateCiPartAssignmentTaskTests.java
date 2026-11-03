/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GenerateCiPartAssignmentTaskTests {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testGeneratesAssignmentFromConfigAndDurations() throws Exception {
        File dir = temp.newFolder();
        File config = new File(dir, "config.json");
        File durations = new File(dir, "durations.json");
        File output = new File(dir, "assignment.json");
        Files.writeString(config.toPath(), """
            { "parts": 2, "workers": 4, "antiAffinity": [[":server:test", ":server:internalClusterTest"]] }
            """);
        Files.writeString(durations.toPath(), """
            { "source": "ignored", "tests": { ":server:test": 600, ":server:internalClusterTest": 590, ":a:test": 20 },
              "projects": { ":server": 120 } }
            """);
        Project project = ProjectBuilder.builder().withProjectDir(dir).build();
        GenerateCiPartAssignmentTask task = project.getTasks().register("gen", GenerateCiPartAssignmentTask.class, t -> {
            t.getConfigFile().set(config);
            t.getDurationsFile().set(durations);
            t.getAssignmentFile().set(output);
        }).get();

        task.generate();

        CiPartAssignment assignment = CiPartAssignment.parse(Files.readString(output.toPath()));
        assertThat(assignment.parts(), equalTo(2));
        assertThat(assignment.partOfTest(":server:test"), not(equalTo(assignment.partOfTest(":server:internalClusterTest"))));
        assertThat(assignment.projects().containsKey(":server"), equalTo(true));
        assertThat(Files.readString(output.toPath()).contains(CiPartAssignment.COMMENT), equalTo(true));
    }
}
