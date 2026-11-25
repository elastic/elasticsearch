/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.Action;
import org.gradle.api.file.ConfigurableFilePermissions;
import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.file.RelativePath;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributionPermissionsTests {

    @Test
    public void modulesPermissions() {
        assertEquals(0755, applyMode(DistributionPermissions.modules("linux"), "tool", "bin", "script"));
        assertEquals(0755, applyMode(DistributionPermissions.modules("darwin"), "bundle", "MacOS", "launcher"));
        assertEquals(0644, applyMode(DistributionPermissions.modules("linux"), "module", "config", "file"));
    }

    @Test
    public void modulesPermissionsForShortPath() {
        assertEquals(0644, applyMode(DistributionPermissions.modules("linux"), "only-one-segment"));
    }

    @Test
    public void jdkPermissionsAndSrcZipExclusion() {
        PermissionProbe binProbe = apply(DistributionPermissions.jdk(), "jdk", "bin", "java");
        assertEquals(0755, binProbe.mode.get());
        assertFalse(binProbe.excluded);

        PermissionProbe helperProbe = apply(DistributionPermissions.jdk(), "jdk", "lib", "jspawnhelper");
        assertEquals(0755, helperProbe.mode.get());

        PermissionProbe srcZipProbe = apply(DistributionPermissions.jdk(), "jdk", "lib", "src.zip");
        assertEquals(0644, srcZipProbe.mode.get());
        assertTrue(srcZipProbe.excluded);

        PermissionProbe shortPathProbe = apply(DistributionPermissions.jdk(), "src.zip");
        assertEquals(0644, shortPathProbe.mode.get());
        assertTrue(shortPathProbe.excluded);
    }

    @Test
    public void serverLauncherPermissions() {
        assertEquals(0755, applyMode(DistributionPermissions.serverLauncher(), "tools", "server-launcher", "server-launcher"));
        assertEquals(0755, applyMode(DistributionPermissions.serverLauncher(), "tools", "server-launcher", "server-launcher.exe"));
        assertEquals(-1, applyMode(DistributionPermissions.serverLauncher(), "tools", "server-launcher", "other.jar"));
    }

    private static int applyMode(Action<FileCopyDetails> action, String... segments) {
        return apply(action, segments).mode.get();
    }

    private static PermissionProbe apply(Action<FileCopyDetails> action, String... segments) {
        PermissionProbe probe = new PermissionProbe(segments);
        action.execute(probe.details);
        return probe;
    }

    /**
     * Captures the permission mode and exclusion a {@link DistributionPermissions} action applies to a single file.
     *
     * <p>{@link FileCopyDetails} and {@link ConfigurableFilePermissions} belong to Gradle's copy pipeline and cannot
     * be instantiated directly in a unit test, so per the AGENTS.md testing guidance they are Mockito mocks rather
     * than hand-rolled proxies. The real copy behaviour is exercised end to end by {@code DistributionLayoutPluginFuncTest}.
     */
    private static final class PermissionProbe {
        private final AtomicInteger mode = new AtomicInteger(-1);
        private boolean excluded;
        private final FileCopyDetails details;

        @SuppressWarnings("unchecked")
        PermissionProbe(String... segments) {
            String name = segments[segments.length - 1];

            ConfigurableFilePermissions permissions = mock(ConfigurableFilePermissions.class);
            doAnswer(invocation -> {
                mode.set((Integer) invocation.getArguments()[0]);
                return null;
            }).when(permissions).unix(anyInt());

            details = mock(FileCopyDetails.class);
            when(details.getRelativePath()).thenReturn(new RelativePath(true, segments));
            when(details.getName()).thenReturn(name);
            doAnswer(invocation -> {
                excluded = true;
                return null;
            }).when(details).exclude();
            doAnswer(invocation -> {
                Action<ConfigurableFilePermissions> configure = (Action<ConfigurableFilePermissions>) invocation.getArguments()[0];
                configure.execute(permissions);
                return null;
            }).when(details).permissions(any());
        }
    }
}
