/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.internal.release.PruneChangelogsTask.DeleteHelper;
import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.internal.release.PruneChangelogsTask.findAndDeleteFiles;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PruneChangelogsTaskTests extends GradleUnitTestCase {

    private GitWrapper gitWrapper;
    private DeleteHelper deleteHelper;

    @Before
    public void setup() {
        gitWrapper = mock(GitWrapper.class);
        deleteHelper = mock(DeleteHelper.class);
    }

    /**
     * Check that if there are no files in the current checkout, then the task does nothing.
     */
    @Test
    public void findAndDeleteFiles_withNoFiles_doesNothing() {
        // when:
        findAndDeleteFiles(gitWrapper, deleteHelper, null, Set.of(), false, Path.of("rootDir"));

        // then:
        verify(gitWrapper, never()).listVersions(anyString());
    }

    /**
     * Check that if there are files in the checkout, but no prior versions in the git
     * history, the task deletes nothing.
     */
    @Test
    public void findAndDeleteFiles_withFilesButNoPriorVersions_deletesNothing() {
        // given:
        when(gitWrapper.listVersions(anyString())).thenReturn(Stream.of());

        // when:
        findAndDeleteFiles(
            gitWrapper,
            deleteHelper,
            QualifiedVersion.of("7.16.0"),
            Set.of(new File("rootDir/docs/changelog/1234.yml")),
            false,
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper).listVersions("v7.*");
        verify(gitWrapper, never()).listFiles(anyString(), anyString());
    }

    /**
     * Check that if there are files in the checkout, but no files in the git history, then
     * the task deletes nothing.
     */
    @Test
    public void findAndDeleteFiles_withFilesButNoHistoricalFiles_deletesNothing() {
        // given:
        when(gitWrapper.listVersions(anyString())).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
        when(gitWrapper.listFiles(anyString(), anyString())).thenAnswer(args -> Stream.of());

        // when:
        findAndDeleteFiles(
            gitWrapper,
            deleteHelper,
            QualifiedVersion.of("7.16.0"),
            Set.of(new File("rootDir/docs/changelog/1234.yml")),
            false,
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper).listVersions("v7.*");
        verify(gitWrapper, times(2)).listFiles(anyString(), anyString());
    }

    /**
     * Check that if there are files to delete, but the user hasn't supplied the confirmation CLI option,
     * then no files are deleted.
     */
    @Test
    public void findAndDeleteFiles_withFilesToDelete_respectCancellation() {
        // given:
        when(gitWrapper.listVersions(anyString())).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
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
            false,
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper).listVersions("v7.*");
        verify(gitWrapper, times(2)).listFiles(anyString(), anyString());
        verify(deleteHelper, never()).deleteFiles(any());
    }

    /**
     * Check that if there are files to delete, and the user has confirmed the delete action
     * via the CLI option, then the files are deleted.
     */
    @Test
    public void findAndDeleteFiles_withFilesToDelete_deletesFiles() {
        // given:
        when(gitWrapper.listVersions(anyString())).thenReturn(Stream.of(QualifiedVersion.of("7.14.0"), QualifiedVersion.of("7.15.0")));
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
            true,
            Path.of("rootDir")
        );

        // then:
        verify(gitWrapper).listVersions("v7.*");
        verify(gitWrapper, times(2)).listFiles(anyString(), anyString());
        verify(deleteHelper).deleteFiles(Set.of(new File("rootDir/docs/changelog/1234.yml"), new File("rootDir/docs/changelog/5678.yml")));
    }
}
