/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SecurityTests extends ESTestCase {

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

    /** can't execute processes */
    public void testProcessExecution() throws Exception {
        assumeTrue("test requires security manager", System.getSecurityManager() != null);
        try {
            Runtime.getRuntime().exec("ls");
            fail("didn't get expected exception");
        } catch (SecurityException expected) {}
    }
}
