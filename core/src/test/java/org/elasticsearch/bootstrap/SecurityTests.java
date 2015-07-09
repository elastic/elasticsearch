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

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.FilePermission;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permissions;

public class SecurityTests extends ElasticsearchTestCase {
    
    /** test generated permissions */
    public void testGeneratedPermissions() throws Exception {
        Path path = createTempDir();
        // make a fake ES home and ensure we only grant permissions to that.
        Path esHome = path.resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("path.home", esHome.toString());
        Settings settings = settingsBuilder.build();

        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            Environment environment = new Environment(settings);
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

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("path.home", path.resolve("home").toString());
        settingsBuilder.put("path.conf", path.resolve("conf").toString());
        settingsBuilder.put("path.plugins", path.resolve("plugins").toString());
        settingsBuilder.putArray("path.data", path.resolve("data1").toString(), path.resolve("data2").toString());
        settingsBuilder.put("path.logs", path.resolve("logs").toString());
        settingsBuilder.put("pidfile", path.resolve("test.pid").toString());
        Settings settings = settingsBuilder.build();

        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        Environment environment;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            environment = new Environment(settings);
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
    
    public void testEnsureExists() throws IOException {
        Path p = createTempDir();

        // directory exists
        Path exists = p.resolve("exists");
        Files.createDirectory(exists);
        Security.ensureDirectoryExists(exists);
        Files.createTempFile(exists, null, null);
    }
    
    public void testEnsureNotExists() throws IOException { 
        Path p = createTempDir();

        // directory does not exist: create it
        Path notExists = p.resolve("notexists");
        Security.ensureDirectoryExists(notExists);
        Files.createTempFile(notExists, null, null);
    }
    
    public void testEnsureRegularFile() throws IOException {
        Path p = createTempDir();

        // regular file
        Path regularFile = p.resolve("regular");
        Files.createFile(regularFile);
        try {
            Security.ensureDirectoryExists(regularFile);
            fail("didn't get expected exception");
        } catch (IOException expected) {}
    }
    
    public void testEnsureSymlink() throws IOException {
        Path p = createTempDir();
        
        Path exists = p.resolve("exists");
        Files.createDirectory(exists);

        // symlink
        Path linkExists = p.resolve("linkExists");
        try {
            Files.createSymbolicLink(linkExists, exists);
        } catch (UnsupportedOperationException | IOException e) {
            assumeNoException("test requires filesystem that supports symbolic links", e);
        } catch (SecurityException e) {
            assumeNoException("test cannot create symbolic links with security manager enabled", e);
        }
        Security.ensureDirectoryExists(linkExists);
        Files.createTempFile(linkExists, null, null);
    }
    
    public void testEnsureBrokenSymlink() throws IOException {
        Path p = createTempDir();

        // broken symlink
        Path brokenLink = p.resolve("brokenLink");
        try {
            Files.createSymbolicLink(brokenLink, p.resolve("nonexistent"));
        } catch (UnsupportedOperationException | IOException e) {
            assumeNoException("test requires filesystem that supports symbolic links", e);
        } catch (SecurityException e) {
            assumeNoException("test cannot create symbolic links with security manager enabled", e);
        }
        try {
            Security.ensureDirectoryExists(brokenLink);
            fail("didn't get expected exception");
        } catch (IOException expected) {}
    }

    /** We only grant this to special jars */
    public void testUnsafeAccess() throws Exception {
        assumeTrue("test requires security manager", System.getSecurityManager() != null);
        try {
            // class could be legitimately loaded, so we might not fail until setAccessible
            Class.forName("sun.misc.Unsafe")
                 .getDeclaredField("theUnsafe")
                 .setAccessible(true);
            fail("didn't get expected exception");
        } catch (SecurityException expected) {
            // ok
        } catch (Exception somethingElse) {
            assumeNoException("perhaps JVM doesn't have Unsafe?", somethingElse);
        }
    }

    /** can't execute processes */
    public void testProcessExecution() throws Exception {
        assumeTrue("test requires security manager", System.getSecurityManager() != null);
        try {
            Runtime.getRuntime().exec("ls");
            fail("didn't get expected exception");
        } catch (SecurityException expected) {}
    }

    /** When a configured dir is a symlink, test that permissions work on link target */
    public void testSymlinkPermissions() throws IOException {
        // see https://github.com/elastic/elasticsearch/issues/12170
        assumeFalse("windows does not automatically grant permission to the target of symlinks", Constants.WINDOWS);
        Path dir = createTempDir();

        Path target = dir.resolve("target");
        Files.createDirectory(target);

        // symlink
        Path link = dir.resolve("link");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException e) {
            assumeNoException("test requires filesystem that supports symbolic links", e);
        } catch (SecurityException e) {
            assumeNoException("test cannot create symbolic links with security manager enabled", e);
        }
        Permissions permissions = new Permissions();
        Security.addPath(permissions, link, "read");
        assertTrue(permissions.implies(new FilePermission(link.toString(), "read")));
        assertTrue(permissions.implies(new FilePermission(link.resolve("foo").toString(), "read")));
        assertTrue(permissions.implies(new FilePermission(target.toString(), "read")));
        assertTrue(permissions.implies(new FilePermission(target.resolve("foo").toString(), "read")));
    }
}
