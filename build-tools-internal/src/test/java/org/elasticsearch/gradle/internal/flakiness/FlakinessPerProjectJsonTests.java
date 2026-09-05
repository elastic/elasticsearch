/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

/**
 * The per-project (configuration-cache compatible) topology carries a project's whole model through a task
 * {@code @Input} string, so the two envelopes it introduces - {@link FlakinessJson.ProjectModel} and
 * {@link FlakinessJson.ProjectTargetsFile} - must round-trip exactly.
 *
 * <p>The interesting part is {@link Path}: unlike the wire records reaching TypeScript, these envelopes stay
 * inside Java and therefore reuse {@link SourceSetInfo}/{@link TestTaskInfo} verbatim, {@code Path} fields and
 * all. This pins that Jackson's built-in {@code java.nio.file.Path} handling really does survive the trip -
 * if it did not, every resolved {@code outputDir} and {@code srcDir} would silently change meaning.
 */
public class FlakinessPerProjectJsonTests {

    @Test
    public void testProjectModelRoundTripsIncludingPaths() {
        FlakinessJson.ProjectModel model = new FlakinessJson.ProjectModel(
            ":x-pack:plugin:logsdb:qa:rolling-upgrade",
            Path.of("/repo/x-pack/plugin/logsdb/qa/rolling-upgrade"),
            List.of(
                new SourceSetInfo(
                    Kinds.SS_JAVA_REST_TEST,
                    List.of(Path.of("/repo/p/src/javaRestTest/java")),
                    List.of(Path.of("/repo/p/src/javaRestTest/resources")),
                    Path.of("/repo/p/build/classes/java/javaRestTest")
                )
            ),
            List.of(
                new TestTaskInfo("javaRestTest", ":p:javaRestTest", false, List.of(Path.of("/repo/p/build/classes/java/javaRestTest"))),
                new TestTaskInfo("v9.6.0#bwcTest", ":p:v9.6.0#bwcTest", true, List.of(Path.of("/repo/p/build/classes/java/javaRestTest")))
            ),
            // classDirs spans main as well as the test source sets: abstract test bases live in main, and the
            // scan can only call a class abstract if it visited that class's own .class file.
            List.of(Path.of("/repo/p/build/classes/java/main"), Path.of("/repo/p/build/classes/java/javaRestTest")),
            true
        );

        FlakinessJson.ProjectModel back = FlakinessJson.parseProjectModel(FlakinessJson.writeProjectModel(model));

        assertThat(back, is(model));
        assertThat(back.sourceSets().get(0).outputDir(), is(Path.of("/repo/p/build/classes/java/javaRestTest")));
        assertThat(back.testTasks().get(0).enabled(), is(false));
        assertThat(back.testTasks().get(1).testClassesDirs(), contains(Path.of("/repo/p/build/classes/java/javaRestTest")));
        assertThat(
            back.classDirs(),
            contains(Path.of("/repo/p/build/classes/java/main"), Path.of("/repo/p/build/classes/java/javaRestTest"))
        );
    }

    @Test
    public void testProjectTargetsRoundTripsAndKeepsRefIndices() {
        BaseTarget target = new BaseTarget(
            ":libs:dissect",
            "test",
            Kinds.TEST,
            "org.elasticsearch.dissect.DissectParserTests",
            null,
            null,
            List.of(":libs:dissect:test"),
            1,
            null
        );
        FlakinessJson.ProjectTargetsFile file = new FlakinessJson.ProjectTargetsFile(
            ":libs:dissect",
            List.of(new FlakinessJson.RefTarget(2, target)),
            List.of(Path.of("/repo/libs/dissect/build/classes/java/main"), Path.of("/repo/libs/dissect/build/classes/java/test"))
        , List.of());

        FlakinessJson.ProjectTargetsFile back = FlakinessJson.parseProjectTargets(FlakinessJson.writeProjectTargets(file));

        // The ref index is what lets the merge step restore ref ordering and compute the global unresolved set.
        assertThat(back, is(file));
        assertThat(back.resolved().get(0).refIndex(), is(2));
        // classDirs must survive the round trip: the scan step unions this field across every project's file.
        assertThat(
            back.classDirs(),
            contains(Path.of("/repo/libs/dissect/build/classes/java/main"), Path.of("/repo/libs/dissect/build/classes/java/test"))
        );
    }
}
