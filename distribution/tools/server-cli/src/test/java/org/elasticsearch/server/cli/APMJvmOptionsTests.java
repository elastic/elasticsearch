/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ESTestCase.WithoutSecurityManager
public class APMJvmOptionsTests extends ESTestCase {

    private Path installDir;
    private Path agentPath;

    @Before
    public void setup() throws IOException, UserException {
        installDir = makeFakeAgentJar();
        agentPath = APMJvmOptions.findAgentJar(installDir.toAbsolutePath().toString());
    }

    @After
    public void cleanup() throws IOException {
        Files.delete(agentPath);
    }

    public void testFindJar() throws IOException {
        assertNotNull(agentPath);

        Path anotherPath = Files.createDirectories(installDir.resolve("another"));
        Path apmPathDir = anotherPath.resolve("modules").resolve("apm");
        Files.createDirectories(apmPathDir);

        assertTrue(
            expectThrows(UserException.class, () -> APMJvmOptions.findAgentJar(anotherPath.toAbsolutePath().toString())).getMessage()
                .contains("Installation is corrupt")
        );
    }

    public void testFileDeleteWorks() throws IOException {
        var tempFile = APMJvmOptions.createTemporaryPropertiesFile(agentPath.getParent());
        var commandLineOption = APMJvmOptions.agentCommandLineOption(agentPath, tempFile);
        var jvmInfo = mock(JvmInfo.class);
        doReturn(new String[] { commandLineOption }).when(jvmInfo).getInputArguments();
        assertTrue(Files.exists(tempFile));
        Node.deleteTemporaryApmConfig(jvmInfo, (e, p) -> fail("Shouldn't hit an exception"));
        assertFalse(Files.exists(tempFile));
    }

    private Path makeFakeAgentJar() throws IOException {
        Path tempFile = createTempFile();
        Path apmPathDir = tempFile.getParent().resolve("modules").resolve("apm");
        Files.createDirectories(apmPathDir);
        Path apmAgentFile = apmPathDir.resolve("elastic-apm-agent-0.0.0.jar");
        Files.move(tempFile, apmAgentFile);

        return tempFile.getParent();
    }
}
