/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class GenerateReleaseNotesTaskTest {
    /**
     * Check that the task does not update git tags if the current version is a snapshot of the first patch release.
     */
    @Test
    public void needsGitTags_withFirstSnapshot_returnsFalse() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.0-SNAPSHOT"), is(false));
    }

    /**
     * Check that the task does update git tags if the current version is a snapshot after the first patch release.
     */
    @Test
    public void needsGitTags_withLaterSnapshot_returnsTrue() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.1-SNAPSHOT"), is(true));
    }

    /**
     * Check that the task does not update git tags if the current version is the first patch release in a minor series.
     */
    @Test
    public void needsGitTags_withFirstPatchRelease_returnsFalse() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.0"), is(false));
    }

    /**
     * Check that the task does update git tags if the current version is later than the first patch release in a minor series.
     */
    @Test
    public void needsGitTags_withLaterPatchRelease_returnsTrue() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.1"), is(true));
    }

    /**
     * Check that the task does not update git tags if the current version is a first alpha prerelease.
     */
    @Test
    public void needsGitTags_withFirsAlphaRelease_returnsFalse() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.0-alpha1"), is(false));
    }

    /**
     * Check that the task does update git tags if the current version is a prerelease after the first alpha.
     */
    @Test
    public void needsGitTags_withLaterAlphaRelease_returnsFalse() {
        assertThat(GenerateReleaseNotesTask.needsGitTags("8.0.0-alpha2"), is(true));
    }
}
