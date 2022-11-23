/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.internal.release.PruneChangelogsTask.DeleteHelper;
import org.gradle.api.GradleException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.OS.WINDOWS;
import static org.elasticsearch.gradle.internal.release.PruneChangelogsTask.findAndDeleteFiles;
import static org.elasticsearch.gradle.internal.release.PruneChangelogsTask.findPreviousVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PruneChangelogsTaskTests {

    private GitWrapper gitWrapper;
    private DeleteHelper deleteHelper;

    @Before
    public void setup() {
        // TODO: Muted on windows until resolved: https://github.com/elastic/elasticsearch/issues/78318
        assumeFalse(OS.current() == WINDOWS);
        gitWrapper = mock(GitWrapper.class);
        deleteHelper = mock(DeleteHelper.class);
    }

    /**
    * Check that if there are no files in the current checkout, then the task does nothing.
    */
    @Test
    public void findAndDeleteFiles_withNoFiles_doesNothing() {
        // when:
        findAndDeleteFiles(gitWrapper, deleteHelper, null, Set.of(), Path.of("rootDir"));

        // then:
        verify(gitWrapper, never()).listVersions(anyString());
    }

    /**
     * Check that if there are files in the checkout, but no prior versions in the git
     * history, the task deletes nothing.
     */
    @Test
    public void findAndDeleteFiles_withNoPriorVersions_deletesNothing() {
        // given:
        when(gitWrapper.listVersions(anyString())).thenAnswer(args -> Stream.of());

        // when:
        findAndDeleteFiles(
            gitWrapper,
            deleteHelper,
            QualifiedVersion.of("7.16.0"),
            Set.of(new File("rootDir/docs/changelog/1234.yml")),
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper, never()).listFiles(anyString(), anyString());
    }

    /**
     * Check that if there are files in the checkout, but no files in the git history, then
     * the task deletes nothing.
     */
    @Test
    public void findAndDeleteFiles_withFilesButNoHistoricalFiles_deletesNothing() {
        // given:
        when(gitWrapper.listVersions("v6.*")).thenReturn(Stream.of(QualifiedVersion.of("6.14.0"), QualifiedVersion.of("6.15.0")));
        when(gitWrapper.listVersions("v7.*")).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
        when(gitWrapper.listFiles(anyString(), anyString())).thenAnswer(args -> Stream.of());

        // when:
        findAndDeleteFiles(
            gitWrapper,
            deleteHelper,
            QualifiedVersion.of("7.16.0"),
            Set.of(new File("rootDir/docs/changelog/1234.yml")),
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper, times(4)).listFiles(anyString(), anyString());
    }

    /**
     * Check that if there are files to delete, then the files are deleted.
     */
    @Test
    public void findAndDeleteFiles_withFilesToDelete_deletesFiles() {
        // given:
        when(gitWrapper.listVersions("v6.*")).thenReturn(Stream.of(QualifiedVersion.of("6.14.0"), QualifiedVersion.of("6.15.0")));
        when(gitWrapper.listVersions("v7.*")).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
        when(gitWrapper.listFiles(anyString(), anyString())).thenAnswer(
            args -> Stream.of("docs/changelog/1234.yml", "docs/changelog/5678.yml")
        );

        // when:
        findAndDeleteFiles(
            gitWrapper,
            deleteHelper,
            QualifiedVersion.of("7.16.0"),
            Set.of(
                new File("rootDir/docs/changelog/1234.yml"),
                new File("rootDir/docs/changelog/5678.yml"),
                new File("rootDir/docs/changelog/9123.yml")
            ),
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper, times(4)).listFiles(anyString(), anyString());
        verify(deleteHelper).deleteFiles(Set.of(new File("rootDir/docs/changelog/1234.yml"), new File("rootDir/docs/changelog/5678.yml")));
    }

    /**
     * Check that if there are files to delete, but some deletes fail, then the task throws an exception.
     */
    @Test
    public void findAndDeleteFiles_withFilesToDeleteButDeleteFails_throwsException() {
        // given:
        when(gitWrapper.listVersions("v6.*")).thenReturn(Stream.of(QualifiedVersion.of("6.14.0"), QualifiedVersion.of("6.15.0")));
        when(gitWrapper.listVersions("v7.*")).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
        when(gitWrapper.listFiles(anyString(), anyString())).thenAnswer(
            args -> Stream.of("docs/changelog/1234.yml", "docs/changelog/5678.yml")
        );
        // Simulate a delete failure
        when(deleteHelper.deleteFiles(any())).thenReturn(Set.of(new File("rootDir/docs/changelog/1234.yml")));

        // when:
        GradleException e = assertThrows(
            GradleException.class,
            () -> findAndDeleteFiles(
                gitWrapper,
                deleteHelper,
                QualifiedVersion.of("7.16.0"),
                Set.of(
                    new File("rootDir/docs/changelog/1234.yml"),
                    new File("rootDir/docs/changelog/5678.yml"),
                    new File("rootDir/docs/changelog/9123.yml")
                ),
                Path.of("rootDir")
            )
        );

        // Use a `Path` so that the test works across platforms
        final Path failedPath = Path.of("docs", "changelog", "1234.yml");
        assertThat(e.getMessage(), equalTo("Failed to delete some files:\n\n\t" + failedPath + "\n"));
    }

    /**
     * Check that when list versions and the current version is at the start of a major series, then the versions for
     * the previous major series are returned.
     */
    @Test
    public void findPreviousVersion_afterStartOfMajorSeries_inspectsCurrentMajorSeries() {
        // given:
        when(gitWrapper.listVersions("v6.*")).thenReturn(Stream.of());
        when(gitWrapper.listVersions("v7.*")).thenReturn(
            Stream.of(
                QualifiedVersion.of("v7.0.0"),
                QualifiedVersion.of("v7.1.0"),
                QualifiedVersion.of("v7.2.0"),
                // Include later version as well, to model the situation where a new major series has been
                // started, but we're still maintaining the prior series.
                QualifiedVersion.of("v7.3.0"),
                QualifiedVersion.of("v8.0.0")
            )
        );

        // when:
        final QualifiedVersion version = QualifiedVersion.of("v7.2.0-SNAPSHOT");
        final List<QualifiedVersion> versions = findPreviousVersion(gitWrapper, version).collect(Collectors.toList());

        // then:
        assertThat(versions, contains(QualifiedVersion.of("v7.0.0"), QualifiedVersion.of("v7.1.0")));
    }
}
