/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.util.Collections;
import java.util.List;

public class XPackExtensionSecurityTests extends ESTestCase {
    /** Test that we can parse the set of permissions correctly for a simple policy */
    public void testParsePermissions() throws Exception {
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/simple-x-pack-extension-security.policy");
        Permissions expected = new Permissions();
        expected.add(new RuntimePermission("queuePrintJob"));
        PermissionCollection actual = InstallXPackExtensionCommand.parsePermissions(testFile, scratch);
        assertEquals(expected, actual);
    }

    /** Test that we can parse the set of permissions correctly for a complex policy */
    public void testParseTwoPermissions() throws Exception {
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/complex-x-pack-extension-security.policy");
        Permissions expected = new Permissions();
        expected.add(new RuntimePermission("getClassLoader"));
        expected.add(new RuntimePermission("closeClassLoader"));
        PermissionCollection actual = InstallXPackExtensionCommand.parsePermissions(testFile, scratch);
        assertEquals(expected, actual);
    }

    /** Test that we can format some simple permissions properly */
    public void testFormatSimplePermission() throws Exception {
        assertEquals("java.lang.RuntimePermission queuePrintJob",
                InstallXPackExtensionCommand.formatPermission(new RuntimePermission("queuePrintJob")));
    }

    /** Test that we can format an unresolved permission properly */
    public void testFormatUnresolvedPermission() throws Exception {
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/unresolved-x-pack-extension-security.policy");
        PermissionCollection actual = InstallXPackExtensionCommand.parsePermissions(testFile, scratch);
        List<Permission> permissions = Collections.list(actual.elements());
        assertEquals(1, permissions.size());
        assertEquals("org.fake.FakePermission fakeName", InstallXPackExtensionCommand.formatPermission(permissions.get(0)));
    }

    /** no guaranteed equals on these classes, we assert they contain the same set */
    private void assertEquals(PermissionCollection expected, PermissionCollection actual) {
        assertEquals(asSet(Collections.list(expected.elements())), asSet(Collections.list(actual.elements())));
    }
}