/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

public class WindowsServiceCliTests extends CommandTestCase {

    Path javaHome;
    Path binDir;
    Path serviceExe;
    Path mgrExe;

    @Override
    protected Command newCommand() {
        return new WindowsServiceCli() {
            @Override
            public boolean addShutdownHook() {
                return false;
            }
        };
    }

    @Before
    public void setupMockJvm() throws Exception {
        javaHome = createTempDir();
        Path javaBin = javaHome.resolve("bin");
        sysprops.put("java.home", javaHome.toString());
        binDir = esHomeDir.resolve("bin");
        Files.createDirectories(binDir);
        serviceExe = binDir.resolve("elasticsearch-service-x64.exe");
        Files.createFile(serviceExe);
        mgrExe = binDir.resolve("elasticsearch-service-mgr.exe");
        Files.createFile(mgrExe);
    }

    public void testMissingExe() throws Exception {
        Files.delete(serviceExe);
        var e = expectThrows(IllegalStateException.class, () -> executeMain("install"));
        assertThat(e.getMessage(), containsString("Missing procrun exe"));
    }
}
