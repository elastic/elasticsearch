/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookupImpl;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.FileData;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class FilesEntitlementTests extends ESTestCase {

    static Settings settings;

    @BeforeClass
    public static void setupRoot() {
        settings = Settings.EMPTY;
    }

    private static final PathLookup TEST_PATH_LOOKUP = new PathLookupImpl(
        Path.of("/home"),
        Path.of("/config"),
        new Path[] { Path.of("/data1"), Path.of("/data2") },
        new Path[] { Path.of("/shared1"), Path.of("/shared2") },
        Path.of("/lib"),
        Path.of("/modules"),
        Path.of("/plugins"),
        Path.of("/logs"),
        Path.of("/tmp"),
        null,
        pattern -> settings.getValues(pattern)
    );

    public void testEmptyBuild() {
        PolicyValidationException pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(List.of()));
        assertEquals("must specify at least one path", pve.getMessage());
        pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(null));
        assertEquals("must specify at least one path", pve.getMessage());
    }

    public void testInvalidRelativeDirectory() {
        var ex = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of((Map.of("relative_path", "foo", "mode", "read", "relative_to", "bar"))))
        );
        assertThat(ex.getMessage(), is("invalid relative directory: bar, valid values: [config, data, home]"));
    }

    public void testFileDataRelativeWithAbsoluteDirectoryFails() {
        var fileData = FileData.ofRelativePath(Path.of(""), PathLookup.BaseDir.DATA, READ_WRITE);
        var dataDirs = fileData.resolvePaths(TEST_PATH_LOOKUP);
        assertThat(dataDirs.toList(), contains(Path.of("/data1/"), Path.of("/data2")));
    }

    public void testFileDataAbsoluteWithRelativeDirectoryFails() {
        var ex = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of((Map.of("path", "foo", "mode", "read"))))
        );

        assertThat(ex.getMessage(), is("'path' [foo] must be absolute"));
    }

    public void testFileDataRelativeWithEmptyDirectory() {
        var ex = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of((Map.of("relative_path", "/foo", "mode", "read", "relative_to", "config"))))
        );

        var ex2 = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of((Map.of("relative_path", "C:\\foo", "mode", "read", "relative_to", "config"))))
        );

        assertThat(ex.getMessage(), is("'relative_path' [/foo] must be relative"));
        assertThat(ex2.getMessage(), is("'relative_path' [C:\\foo] must be relative"));
    }

    public void testPathSettingResolve() {
        var entitlement = FilesEntitlement.build(
            List.of(Map.of("path_setting", "foo.bar", "basedir_if_relative", "config", "mode", "read"))
        );
        var filesData = entitlement.filesData();
        assertThat(filesData, contains(FileData.ofPathSetting("foo.bar", CONFIG, READ)));

        var fileData = FileData.ofPathSetting("foo.bar", CONFIG, READ);
        // empty settings
        assertThat(fileData.resolvePaths(TEST_PATH_LOOKUP).toList(), empty());

        fileData = FileData.ofPathSetting("foo.bar", CONFIG, READ);
        settings = Settings.builder().put("foo.bar", "/setting/path").build();
        assertThat(fileData.resolvePaths(TEST_PATH_LOOKUP).toList(), contains(Path.of("/setting/path")));

        fileData = FileData.ofPathSetting("foo.*.bar", CONFIG, READ);
        settings = Settings.builder().put("foo.baz.bar", "/setting/path").build();
        assertThat(fileData.resolvePaths(TEST_PATH_LOOKUP).toList(), contains(Path.of("/setting/path")));

        fileData = FileData.ofPathSetting("foo.*.bar", CONFIG, READ);
        settings = Settings.builder().put("foo.baz.bar", "/setting/path").put("foo.baz2.bar", "/other/path").build();
        assertThat(fileData.resolvePaths(TEST_PATH_LOOKUP).toList(), containsInAnyOrder(Path.of("/setting/path"), Path.of("/other/path")));

        fileData = FileData.ofPathSetting("foo.bar", CONFIG, READ);
        settings = Settings.builder().put("foo.bar", "relative_path").build();
        assertThat(fileData.resolvePaths(TEST_PATH_LOOKUP).toList(), contains(Path.of("/config/relative_path")));
    }

    public void testPathSettingBasedirValidation() {
        var e = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of(Map.of("path", "/foo", "mode", "read", "basedir_if_relative", "config")))
        );
        assertThat(e.getMessage(), is("'basedir_if_relative' may only be used with 'path_setting'"));

        e = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(
                List.of(Map.of("relative_path", "foo", "relative_to", "config", "mode", "read", "basedir_if_relative", "config"))
            )
        );
        assertThat(e.getMessage(), is("'basedir_if_relative' may only be used with 'path_setting'"));
    }

    public void testExclusiveParsing() throws Exception {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
                    entitlement-module-name:
                      - files:
                        - path: /test
                          mode: read
                          exclusive: true
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", true).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(FilesEntitlement.build(List.of(Map.of("path", "/test", "mode", "read", "exclusive", true))))
                )
            )
        );
        assertEquals(expected, parsedPolicy);
    }
}
