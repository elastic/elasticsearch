/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.FilePermission;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.util.Set;

@SuppressForbidden(reason = "modifies system properties and attempts to create symbolic links intentionally")
public class EvilSecurityTests extends ESTestCase {

    /** test generated permissions */
    public void testGeneratedPermissions() throws Exception {
        Path path = createTempDir();
        // make a fake ES home and ensure we only grant permissions to that.
        Path esHome = path.resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            Environment environment = TestEnvironment.newEnvironment(settings);
            permissions = Security.createPermissions(environment);
        } finally {
            System.setProperty("java.io.tmpdir", realTmpDir);
        }

        // the fake es home
        assertNoPermissions(esHome, permissions);
        // its parent
        assertNoPermissions(esHome.getParent(), permissions);
        // some other sibling
        assertNoPermissions(esHome.getParent().resolve("other"), permissions);
        // double check we overwrote java.io.tmpdir correctly for the test
        assertNoPermissions(PathUtils.get(realTmpDir), permissions);
    }

    /** test generated permissions for all configured paths */
    @SuppressWarnings("deprecation") // needs to check settings for deprecated path
    @SuppressForbidden(reason = "to create FilePermission object")
    public void testEnvironmentPaths() throws Exception {
        Path path = createTempDir();
        // make a fake ES home and ensure we only grant permissions to that.
        Path esHome = path.resolve("esHome");

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.resolve("home").toString());
        settingsBuilder.put(Environment.PATH_DATA_SETTING.getKey(), esHome.resolve("data1").toString());
        settingsBuilder.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), esHome.resolve("custom").toString());
        settingsBuilder.put(Environment.PATH_LOGS_SETTING.getKey(), esHome.resolve("logs").toString());
        settingsBuilder.put(Environment.NODE_PIDFILE_SETTING.getKey(), esHome.resolve("test.pid").toString());
        Settings settings = settingsBuilder.build();

        Path fakeTmpDir = createTempDir();
        String realTmpDir = System.getProperty("java.io.tmpdir");
        Permissions permissions;
        Environment environment;
        try {
            System.setProperty("java.io.tmpdir", fakeTmpDir.toString());
            environment = new Environment(settings, esHome.resolve("conf"));
            permissions = Security.createPermissions(environment);
        } finally {
            System.setProperty("java.io.tmpdir", realTmpDir);
        }

        // the fake es home
        assertNoPermissions(esHome, permissions);
        // its parent
        assertNoPermissions(esHome.getParent(), permissions);
        // some other sibling
        assertNoPermissions(esHome.getParent().resolve("other"), permissions);
        // double check we overwrote java.io.tmpdir correctly for the test
        assertNoPermissions(PathUtils.get(realTmpDir), permissions);

        // check that all directories got permissions:

        // bin file: ro
        assertExactPermissions(new FilePermission(environment.binFile().toString(), "read,readlink"), permissions);
        // lib file: ro
        assertExactPermissions(new FilePermission(environment.libFile().toString(), "read,readlink"), permissions);
        // modules file: ro
        assertExactPermissions(new FilePermission(environment.modulesFile().toString(), "read,readlink"), permissions);
        // config file: ro
        assertExactPermissions(new FilePermission(environment.configFile().toString(), "read,readlink"), permissions);
        // plugins: ro
        assertExactPermissions(new FilePermission(environment.pluginsFile().toString(), "read,readlink"), permissions);

        // data paths: r/w
        assertExactPermissions(new FilePermission(environment.dataFile().toString(), "read,readlink,write,delete"), permissions);
        assertExactPermissions(new FilePermission(environment.sharedDataFile().toString(), "read,readlink,write,delete"), permissions);
        // logs: r/w
        assertExactPermissions(new FilePermission(environment.logsFile().toString(), "read,readlink,write,delete"), permissions);
        // temp dir: r/w
        assertExactPermissions(new FilePermission(fakeTmpDir.toString(), "read,readlink,write,delete"), permissions);
        // PID file: delete only (for the shutdown hook)
        assertExactPermissions(new FilePermission(environment.pidFile().toString(), "delete"), permissions);
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
        FilePermissionUtils.addDirectoryPath(permissions, "testing", link, "read", false);
        assertExactPermissions(new FilePermission(link.toString(), "read"), permissions);
        assertExactPermissions(new FilePermission(link.resolve("foo").toString(), "read"), permissions);
        assertExactPermissions(new FilePermission(target.toString(), "read"), permissions);
        assertExactPermissions(new FilePermission(target.resolve("foo").toString(), "read"), permissions);
    }

    /**
     * checks exact file permissions, meaning those and only those for that path.
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    static void assertExactPermissions(FilePermission expected, PermissionCollection actual) {
        String target = expected.getName(); // see javadocs
        Set<String> permissionSet = asSet(expected.getActions().split(","));
        boolean read = permissionSet.remove("read");
        boolean readlink = permissionSet.remove("readlink");
        boolean write = permissionSet.remove("write");
        boolean delete = permissionSet.remove("delete");
        boolean execute = permissionSet.remove("execute");
        assertTrue("unrecognized permission: " + permissionSet, permissionSet.isEmpty());
        assertEquals(read, actual.implies(new FilePermission(target, "read")));
        assertEquals(readlink, actual.implies(new FilePermission(target, "readlink")));
        assertEquals(write, actual.implies(new FilePermission(target, "write")));
        assertEquals(delete, actual.implies(new FilePermission(target, "delete")));
        assertEquals(execute, actual.implies(new FilePermission(target, "execute")));
    }

    /**
     * checks that this path has no permissions
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    static void assertNoPermissions(Path path, PermissionCollection actual) {
        String target = path.toString();
        assertFalse(actual.implies(new FilePermission(target, "read")));
        assertFalse(actual.implies(new FilePermission(target, "readlink")));
        assertFalse(actual.implies(new FilePermission(target, "write")));
        assertFalse(actual.implies(new FilePermission(target, "delete")));
        assertFalse(actual.implies(new FilePermission(target, "execute")));
    }
}
