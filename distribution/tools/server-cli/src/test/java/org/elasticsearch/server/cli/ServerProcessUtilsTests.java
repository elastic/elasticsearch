/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class ServerProcessUtilsTests extends ESTestCase {

    protected final Map<String, String> sysprops = new HashMap<>();
    protected final Map<String, String> envVars = new HashMap<>();

    @Before
    public void resetEnv() {
        sysprops.clear();
        sysprops.put("os.name", "Linux");
        sysprops.put("java.home", "javahome");
        envVars.clear();
    }

    private ProcessInfo createProcessInfo() {
        return new ProcessInfo(Map.copyOf(sysprops), Map.copyOf(envVars), Path.of("."));
    }

    public void testTempDir() throws Exception {
        var tmpDir = ServerProcessUtils.setupTempDir(createProcessInfo());
        assertThat(tmpDir.toString(), Files.exists(tmpDir), is(true));
        assertThat(tmpDir.getFileName().toString(), startsWith("elasticsearch-"));
    }

    public void testTempDirWindows() throws Exception {
        Path baseTmpDir = createTempDir();
        sysprops.put("os.name", "Windows 10");
        sysprops.put("java.io.tmpdir", baseTmpDir.toString());
        var tmpDir = ServerProcessUtils.setupTempDir(createProcessInfo());
        assertThat(tmpDir.toString(), Files.exists(tmpDir), is(true));
        assertThat(tmpDir.getFileName().toString(), equalTo("elasticsearch"));
        assertThat(tmpDir.getParent().toString(), equalTo(baseTmpDir.toString()));
    }

    public void testTempDirOverride() throws Exception {
        Path customTmpDir = createTempDir();
        envVars.put("ES_TMPDIR", customTmpDir.toString());
        var tmpDir = ServerProcessUtils.setupTempDir(createProcessInfo());
        assertThat(tmpDir.toString(), equalTo(customTmpDir.toString()));
    }

    public void testTempDirOverrideMissing() {
        Path baseDir = createTempDir();
        envVars.put("ES_TMPDIR", baseDir.resolve("dne").toString());
        var e = expectThrows(UserException.class, () -> ServerProcessUtils.setupTempDir(createProcessInfo()));
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(e.getMessage(), containsString("dne] does not exist"));
    }

    public void testTempDirOverrideNotADirectory() throws Exception {
        Path tmpFile = createTempFile();
        envVars.put("ES_TMPDIR", tmpFile.toString());
        var e = expectThrows(UserException.class, () -> ServerProcessUtils.setupTempDir(createProcessInfo()));
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(e.getMessage(), containsString("is not a directory"));
    }
}
