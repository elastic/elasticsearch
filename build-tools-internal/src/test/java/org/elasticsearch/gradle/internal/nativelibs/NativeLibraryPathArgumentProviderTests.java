/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NativeLibraryPathArgumentProviderTests {

    private Project project;
    private NativeLibraryPathArgumentProvider provider;

    @Before
    public void setUp() {
        project = ProjectBuilder.builder().build();
        provider = project.getObjects().newInstance(NativeLibraryPathArgumentProvider.class);
    }

    /**
     * The property is passed even when the list is empty, because without it LoaderHelper looks for the native
     * libraries under es.path.home, which a forked test or benchmark JVM does not set.
     */
    @Test
    public void testPassesAnEmptyPathWhenThereAreNoLibraryDirectories() {
        assertEquals(List.of("-Des.nativelibs.path="), provider.asArguments());
    }

    @Test
    public void testListsTheCurrentPlatformSubdirectoryOfEveryLibraryDirectory() {
        File alpha = project.file("alpha");
        File beta = project.file("beta");
        provider.getLibraryDirectories().from(alpha, beta);

        String platform = BuildNativeLibraryTask.hostPlatform();
        String expected = new File(alpha, platform).getPath() + File.pathSeparator + new File(beta, platform).getPath();
        assertEquals(List.of("-Des.nativelibs.path=" + expected), provider.asArguments());
    }
}
