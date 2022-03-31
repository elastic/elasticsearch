/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.docker.DockerRun;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.docker.Docker.removeContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runContainerExpectingFailure;
import static org.elasticsearch.packaging.util.docker.Docker.waitForElasticsearch;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class TemporaryDirectoryConfigTests extends PackagingTestCase {

    @Before
    public void onlyLinux() {
        assumeTrue("only Linux", distribution.platform == Distribution.Platform.LINUX);
    }

    @After
    public void cleanupContainer() {
        if (distribution().isDocker()) {
            removeContainer();
        }
    }

    public void test10Install() throws Exception {
        install();
        setFileSuperuser("test_superuser", "test_superuser_password");
    }

    public void test20AcceptsCustomPath() throws Exception {
        assumeFalse(distribution().isDocker());

        final Path tmpDir = createTempDir("libffi");
        sh.getEnv().put("LIBFFI_TMPDIR", tmpDir.toString());
        withLibffiTmpdir(tmpDir.toString(), confPath -> assertWhileRunning(() -> {
            ServerUtils.makeRequest(
                Request.Get("https://localhost:9200/"),
                "test_superuser",
                "test_superuser_password",
                ServerUtils.getCaCert(confPath)
            ); // just checking it doesn't throw
        }));
    }

    public void test21AcceptsCustomPathInDocker() throws Exception {
        assumeTrue(distribution().isDocker());

        final Path tmpDir = createTempDir("libffi");

        installation = runContainer(
            distribution(),
            DockerRun.builder()
                // There's no actual need for this to be a bind-mounted dir, but it's the quickest
                // way to create a directory in the container before the entrypoint runs.
                .volume(tmpDir, tmpDir)
                .envVar("ELASTIC_PASSWORD", "nothunter2")
                .envVar("LIBFFI_TMPDIR", tmpDir.toString())
        );

        waitForElasticsearch(installation, "elastic", "nothunter2");
    }

    public void test30VerifiesCustomPath() throws Exception {
        assumeFalse(distribution().isDocker());

        final Path tmpFile = createTempDir("libffi").resolve("file");
        Files.createFile(tmpFile);
        withLibffiTmpdir(
            tmpFile.toString(),
            confPath -> assertElasticsearchFailure(runElasticsearchStartCommand(null, false, false), "LIBFFI_TMPDIR", null)
        );
    }

    public void test31VerifiesCustomPathInDocker() throws Exception {
        assumeTrue(distribution().isDocker());

        final Path tmpDir = createTempDir("libffi");
        final Path tmpFile = tmpDir.resolve("file");
        Files.createFile(tmpFile);

        final Shell.Result result = runContainerExpectingFailure(
            distribution(),
            DockerRun.builder().volume(tmpDir, tmpDir).envVar("LIBFFI_TMPDIR", tmpFile.toString())
        );
        assertThat(result.stderr(), containsString("LIBFFI_TMPDIR"));
    }

    private void withLibffiTmpdir(String tmpDir, CheckedConsumer<Path, Exception> action) throws Exception {
        sh.getEnv().put("LIBFFI_TMPDIR", tmpDir);
        withCustomConfig(confPath -> {
            if (distribution.isPackage()) {
                append(installation.envFile, "LIBFFI_TMPDIR=" + tmpDir);
            }
            action.accept(confPath);
        });

    }
}
