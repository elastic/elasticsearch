/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.runtime.policy.DirectoryResolver;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class EnvironmentDirectoryResolverTests extends ESTestCase {

    public void testDataDirs() {
        var resolver = createResolver();
        var dataDirs = resolver.resolveData(Path.of("foo")).toList();
        assertThat(dataDirs, contains(Path.of("/data1/foo"), Path.of("/data2/foo")));
    }

    public void testConfigDir() {
        var resolver = createResolver();
        var configDir = resolver.resolveConfig(Path.of("foo"));
        assertThat(configDir, is(Path.of("/config/foo")));
    }

    public void testTempDir() {
        var resolver = createResolver();
        var tempDir = resolver.resolveTemp(Path.of("foo"));
        assertThat(tempDir, is(Path.of("/tmp/foo")));
    }

    private static DirectoryResolver createResolver() {
        return new EntitlementInitialization.EnvironmentDirectoryResolver(
            new EntitlementBootstrap.BootstrapArgs(
                Map.of(),
                c -> null,
                new Path[] { Path.of("/data1"), Path.of("/data2") },
                Path.of("/config"),
                Path.of("/tmp")
            )
        );
    }
}
