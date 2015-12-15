/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.util.Collections;
import java.util.List;

/** Tests plugin manager security check */
public class PluginSecurityTests extends ESTestCase {
    
    /** Test that we can parse the set of permissions correctly for a simple policy */
    public void testParsePermissions() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/simple-plugin-security.policy");
        Permissions expected = new Permissions();
        expected.add(new RuntimePermission("queuePrintJob"));
        PermissionCollection actual = PluginSecurity.parsePermissions(Terminal.DEFAULT, testFile, scratch);
        assertEquals(expected, actual);
    }
    
    /** Test that we can parse the set of permissions correctly for a complex policy */
    public void testParseTwoPermissions() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/complex-plugin-security.policy");
        Permissions expected = new Permissions();
        expected.add(new RuntimePermission("getClassLoader"));
        expected.add(new RuntimePermission("closeClassLoader"));
        PermissionCollection actual = PluginSecurity.parsePermissions(Terminal.DEFAULT, testFile, scratch);
        assertEquals(expected, actual);
    }
    
    /** Test that we can format some simple permissions properly */
    public void testFormatSimplePermission() throws Exception {
        assertEquals("java.lang.RuntimePermission queuePrintJob", PluginSecurity.formatPermission(new RuntimePermission("queuePrintJob")));
    }
    
    /** Test that we can format an unresolved permission properly */
    public void testFormatUnresolvedPermission() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/unresolved-plugin-security.policy");
        PermissionCollection actual = PluginSecurity.parsePermissions(Terminal.DEFAULT, testFile, scratch);
        List<Permission> permissions = Collections.list(actual.elements());
        assertEquals(1, permissions.size());
        assertEquals("org.fake.FakePermission fakeName", PluginSecurity.formatPermission(permissions.get(0)));
    }
    
    /** no guaranteed equals on these classes, we assert they contain the same set */
    private void assertEquals(PermissionCollection expected, PermissionCollection actual) {
        assertEquals(asSet(Collections.list(expected.elements())), asSet(Collections.list(actual.elements())));
    }
}
