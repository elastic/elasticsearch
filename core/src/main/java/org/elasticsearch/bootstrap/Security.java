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

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.file.AccessMode;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.security.Permissions;
import java.security.Policy;

/** 
 * Initializes securitymanager with necessary permissions.
 * <p>
 * We use a template file (the one we test with), and add additional 
 * permissions based on the environment (data paths, etc)
 */
final class Security {
       
    /** 
     * Initializes securitymanager for the environment
     * Can only happen once!
     */
    static void configure(Environment environment) throws Exception {
        // enable security policy: union of template and environment-based paths.
        Policy.setPolicy(new ESPolicy(createPermissions(environment)));

        // enable security manager
        System.setSecurityManager(new SecurityManager());

        // do some basic tests
        selfTest();
    }

    /** returns dynamic Permissions to configured paths */
    static Permissions createPermissions(Environment environment) throws IOException {
        // TODO: improve test infra so we can reduce permissions where read/write
        // is not really needed...
        Permissions policy = new Permissions();
        addPath(policy, environment.tmpFile(), "read,readlink,write,delete");
        addPath(policy, environment.homeFile(), "read,readlink,write,delete");
        addPath(policy, environment.configFile(), "read,readlink,write,delete");
        addPath(policy, environment.logsFile(), "read,readlink,write,delete");
        addPath(policy, environment.pluginsFile(), "read,readlink,write,delete");
        for (Path path : environment.dataFiles()) {
            addPath(policy, path, "read,readlink,write,delete");
        }
        for (Path path : environment.dataWithClusterFiles()) {
            addPath(policy, path, "read,readlink,write,delete");
        }
        for (Path path : environment.repoFiles()) {
            addPath(policy, path, "read,readlink,write,delete");
        }
        if (environment.pidFile() != null) {
            addPath(policy, environment.pidFile().getParent(), "read,readlink,write,delete");
        }
        return policy;
    }
    
    /** Add access to path (and all files underneath it */
    static void addPath(Permissions policy, Path path, String permissions) throws IOException {
        // paths may not exist yet
        ensureDirectoryExists(path);

        // add each path twice: once for itself, again for files underneath it
        policy.add(new FilePermission(path.toString(), permissions));
        policy.add(new FilePermission(path.toString() + path.getFileSystem().getSeparator() + "-", permissions));
    }
    
    /**
     * Ensures configured directory {@code path} exists.
     * @throws IOException if {@code path} exists, but is not a directory, not accessible, or broken symbolic link.
     */
    static void ensureDirectoryExists(Path path) throws IOException {
        // this isn't atomic, but neither is createDirectories.
        if (Files.isDirectory(path)) {
            // verify access, following links (throws exception if something is wrong)
            // we only check READ as a sanity test
            path.getFileSystem().provider().checkAccess(path.toRealPath(), AccessMode.READ);
        } else {
            // doesn't exist, or not a directory
            try {
                Files.createDirectories(path);
            } catch (FileAlreadyExistsException e) {
                // convert optional specific exception so the context is clear
                IOException e2 = new NotDirectoryException(path.toString());
                e2.addSuppressed(e);
                throw e2;
            }
        }
    }

    /** Simple checks that everything is ok */
    @SuppressForbidden(reason = "accesses jvm default tempdir as a self-test")
    static void selfTest() throws IOException {
        // check we can manipulate temporary files
        try {
            Path p = Files.createTempFile(null, null);
            try {
                Files.delete(p);
            } catch (IOException ignored) {
                // potentially virus scanner
            }
        } catch (SecurityException problem) {
            throw new SecurityException("Security misconfiguration: cannot access java.io.tmpdir", problem);
        }
    }
}
