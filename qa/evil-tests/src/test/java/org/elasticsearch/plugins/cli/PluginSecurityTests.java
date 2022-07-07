/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.bootstrap.PluginPolicyInfo;
import org.elasticsearch.bootstrap.PolicyUtil;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.PropertyPermission;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests plugin manager security check */
public class PluginSecurityTests extends ESTestCase {

    PluginPolicyInfo makeDummyPlugin(String policy, String... files) throws IOException {
        Path plugin = createTempDir();
        Files.copy(this.getDataPath(policy), plugin.resolve(PluginDescriptor.ES_PLUGIN_POLICY));
        for (String file : files) {
            Files.createFile(plugin.resolve(file));
        }
        return PolicyUtil.getPluginPolicyInfo(plugin, createTempDir());
    }

    /** Test that we can parse the set of permissions correctly for a simple policy */
    public void testParsePermissions() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path scratch = createTempDir();
        PluginPolicyInfo info = makeDummyPlugin("simple-plugin-security.policy");
        Set<String> actual = PluginSecurity.getPermissionDescriptions(info, scratch);
        assertThat(actual, contains(PluginSecurity.formatPermission(new PropertyPermission("someProperty", "read"))));
    }

    /** Test that we can parse the set of permissions correctly for a complex policy */
    public void testParseTwoPermissions() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path scratch = createTempDir();
        PluginPolicyInfo info = makeDummyPlugin("complex-plugin-security.policy");
        Set<String> actual = PluginSecurity.getPermissionDescriptions(info, scratch);
        assertThat(
            actual,
            containsInAnyOrder(
                PluginSecurity.formatPermission(new RuntimePermission("getClassLoader")),
                PluginSecurity.formatPermission(new RuntimePermission("createClassLoader"))
            )
        );
    }

    /** Test that we can format some simple permissions properly */
    public void testFormatSimplePermission() throws Exception {
        assertEquals(
            "java.lang.RuntimePermission accessDeclaredMembers",
            PluginSecurity.formatPermission(new RuntimePermission("accessDeclaredMembers"))
        );
    }
}
