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

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        action.execute(probe.details());
        return probe;
    }

    private static class PermissionProbe {
        private final String[] segments;
        private final String name;
        private final AtomicInteger mode = new AtomicInteger(-1);
        private boolean excluded;

        PermissionProbe(String... segments) {
            this.segments = segments;
            this.name = segments[segments.length - 1];
        }

        @SuppressWarnings("unchecked")
        FileCopyDetails details() {
            return (FileCopyDetails) Proxy.newProxyInstance(
                FileCopyDetails.class.getClassLoader(),
                new Class[] { FileCopyDetails.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRelativePath" -> new RelativePath(true, segments);
                    case "getName" -> name;
                    case "exclude" -> {
                        excluded = true;
                        yield null;
                    }
                    case "permissions" -> {
                        Action<ConfigurableFilePermissions> action = (Action<ConfigurableFilePermissions>) args[0];
                        action.execute(permissions());
                        yield null;
                    }
                    case "toString" -> "PermissionProbe(" + String.join("/", segments) + ")";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
            );
        }

        @SuppressWarnings("unchecked")
        private ConfigurableFilePermissions permissions() {
            return (ConfigurableFilePermissions) Proxy.newProxyInstance(
                ConfigurableFilePermissions.class.getClassLoader(),
                new Class[] { ConfigurableFilePermissions.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "unix" -> {
                        if (args.length == 1 && args[0] instanceof Integer unixMode) {
                            mode.set(unixMode);
                        }
                        yield null;
                    }
                    case "toString" -> "Permissions(" + mode.get() + ")";
                    default -> null;
                }
            );
        }
    }
}
