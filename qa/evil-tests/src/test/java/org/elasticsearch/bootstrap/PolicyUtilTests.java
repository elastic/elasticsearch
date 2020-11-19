/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Policy;
import java.security.URIParameter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasKey;

public class PolicyUtilTests extends ESTestCase {

    @Before
    public void assumeSecurityManagerDisabled() {
        assumeTrue(
            "test cannot run with security manager enabled",
            System.getSecurityManager() == null);
    }

    URL makeUrl(String s) {
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    Path makeDummyPlugin(String policy, String... files) throws IOException {
        Path plugin = createTempDir();
        Files.copy(this.getDataPath(policy), plugin.resolve(PluginInfo.ES_PLUGIN_POLICY));
        for (String file : files) {
            Files.createFile(plugin.resolve(file));
        }
        return plugin;
    }

    @SuppressForbidden(reason = "set for test")
    void setProperty(String key, String value) {
        System.setProperty(key, value);
    }

    @SuppressForbidden(reason = "cleanup test")
    void clearProperty(String key) {
        System.clearProperty(key);
    }

    public void testCodebaseJarMap() throws Exception {
        Set<URL> urls = new LinkedHashSet<>(List.of(
            makeUrl("file:///foo.jar"),
            makeUrl("file:///bar.txt"),
            makeUrl("file:///a/bar.jar")
        ));

        Map<String, URL> jarMap = PolicyUtil.getCodebaseJarMap(urls);
        assertThat(jarMap, hasKey("foo.jar"));
        assertThat(jarMap, hasKey("bar.jar"));
        // only jars are grabbed
        assertThat(jarMap, not(hasKey("bar.txt")));

        // order matters
        assertThat(jarMap.keySet(), contains("foo.jar", "bar.jar"));
    }

    public void testPluginPolicyInfoEmpty() throws Exception {
        assertThat(PolicyUtil.getPluginPolicyInfo(createTempDir()), is(nullValue()));
    }

    public void testPluginPolicyInfoNoJars() throws Exception {
        Path noJarsPlugin = makeDummyPlugin("dummy.policy");
        PluginPolicyInfo info = PolicyUtil.getPluginPolicyInfo(noJarsPlugin);
        assertThat(info.policy, is(not(nullValue())));
        assertThat(info.jars, emptyIterable());
    }

    public void testPluginPolicyInfo() throws Exception {
        Path plugin = makeDummyPlugin("dummy.policy",
            "foo.jar", "foo.txt", "bar.jar");
        PluginPolicyInfo info = PolicyUtil.getPluginPolicyInfo(plugin);
        assertThat(info.policy, is(not(nullValue())));
        assertThat(info.jars, containsInAnyOrder(
            plugin.resolve("foo.jar").toUri().toURL(),
            plugin.resolve("bar.jar").toUri().toURL()));
    }

    public void testPolicyMissingCodebaseProperty() throws Exception {
        Path plugin = makeDummyPlugin("missing-codebase.policy", "foo.jar");
        URL policyFile = plugin.resolve(PluginInfo.ES_PLUGIN_POLICY).toUri().toURL();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PolicyUtil.readPolicy(policyFile, Map.of()));
        assertThat(e.getMessage(), containsString("Unknown codebases [codebase.doesnotexist] in policy file"));
    }

    public void testPolicyPermissions() throws Exception {
        Path plugin = makeDummyPlugin("global-and-jar.policy", "foo.jar", "bar.jar");
        Path tmpDir = createTempDir();
        try {
            URL jarUrl = plugin.resolve("foo.jar").toUri().toURL();
            setProperty("jarUrl", jarUrl.toString());
            URL policyFile = plugin.resolve(PluginInfo.ES_PLUGIN_POLICY).toUri().toURL();
            Policy policy = Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));

            Set<Permission> globalPermissions = PolicyUtil.getPolicyPermissions(null, policy, tmpDir);
            assertThat(globalPermissions, contains(new RuntimePermission("queuePrintJob")));

            Set<Permission> jarPermissions = PolicyUtil.getPolicyPermissions(jarUrl, policy, tmpDir);
            assertThat(jarPermissions,
                containsInAnyOrder(new RuntimePermission("getClassLoader"), new RuntimePermission("queuePrintJob")));
        } finally {
            clearProperty("jarUrl");
        }
    }
}
