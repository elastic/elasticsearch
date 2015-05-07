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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.FilePermission;
import java.nio.file.Path;
import java.security.Permissions;

public class SecurityTests extends ElasticsearchTestCase {
    
    /** test generated permissions */
    public void testGeneratedPermissions() throws Exception {
        Path path = createTempDir();
        // make a fake ES home and ensure we only grant permissions to that.
        Path esHome = path.resolve("esHome");
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        settingsBuilder.put("path.home", esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = new Environment(settings);
        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            permissions = Security.createPermissions(environment);
        } finally {
            System.setProperty("java.io.tmpdir", realTmpDir);
        }
      
        // the fake es home
        assertTrue(permissions.implies(new FilePermission(esHome.toString(), "read")));
        // its parent
        assertFalse(permissions.implies(new FilePermission(path.toString(), "read")));
        // some other sibling
        assertFalse(permissions.implies(new FilePermission(path.resolve("other").toString(), "read")));
        // double check we overwrote java.io.tmpdir correctly for the test
        assertFalse(permissions.implies(new FilePermission(realTmpDir.toString(), "read")));
    }

    /** test generated permissions for all configured paths */
    public void testEnvironmentPaths() throws Exception {
        Path path = createTempDir();

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        settingsBuilder.put("path.home", path.resolve("home").toString());
        settingsBuilder.put("path.conf", path.resolve("conf").toString());
        settingsBuilder.put("path.plugins", path.resolve("plugins").toString());
        settingsBuilder.putArray("path.data", path.resolve("data1").toString(), path.resolve("data2").toString());
        settingsBuilder.put("path.logs", path.resolve("logs").toString());
        settingsBuilder.put("pidfile", path.resolve("test.pid").toString());
        Settings settings = settingsBuilder.build();

        Environment environment = new Environment(settings);
        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            permissions = Security.createPermissions(environment);
        } finally {
            System.setProperty("java.io.tmpdir", realTmpDir);
        }

        // check that all directories got permissions:
        // homefile: this is needed unless we break out rules for "lib" dir.
        // TODO: make read-only
        assertTrue(permissions.implies(new FilePermission(environment.homeFile().toString(), "read,readlink,write,delete")));
        // config file
        // TODO: make read-only
        assertTrue(permissions.implies(new FilePermission(environment.configFile().toString(), "read,readlink,write,delete")));
        // plugins: r/w, TODO: can this be minimized?
        assertTrue(permissions.implies(new FilePermission(environment.pluginsFile().toString(), "read,readlink,write,delete")));
        // data paths: r/w
        for (Path dataPath : environment.dataFiles()) {
            assertTrue(permissions.implies(new FilePermission(dataPath.toString(), "read,readlink,write,delete")));
        }
        for (Path dataPath : environment.dataWithClusterFiles()) {
            assertTrue(permissions.implies(new FilePermission(dataPath.toString(), "read,readlink,write,delete")));
        }
        // logs: r/w
        assertTrue(permissions.implies(new FilePermission(environment.logsFile().toString(), "read,readlink,write,delete")));
        // temp dir: r/w
        assertTrue(permissions.implies(new FilePermission(fakeTmpDir.toString(), "read,readlink,write,delete")));
        // double check we overwrote java.io.tmpdir correctly for the test
        assertFalse(permissions.implies(new FilePermission(realTmpDir.toString(), "read")));
        // PID file: r/w
        assertTrue(permissions.implies(new FilePermission(environment.pidFile().toString(), "read,readlink,write,delete")));
    }
}
