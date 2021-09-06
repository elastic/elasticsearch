/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assume.assumeFalse;

public class PasswordToolsTests extends PackagingTestCase {

    private static final Pattern USERPASS_REGEX = Pattern.compile("PASSWORD (\\w+) = ([^\\s]+)");
    private static final String BOOTSTRAP_PASSWORD = "myS3curepass";

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    public void test010Install() throws Exception {
        install();
        // Enable security for this test only where it is necessary, until we can enable it for all
        ServerUtils.enableSecurityFeatures(installation);
    }

    public void test20GeneratePasswords() throws Exception {
        assertWhileRunning(() -> {
            ServerUtils.waitForElasticsearch(installation);
            Shell.Result result = installation.executables().setupPasswordsTool.run("auto --batch", null);
            Map<String, String> userpasses = parseUsersAndPasswords(result.stdout);
            for (Map.Entry<String, String> userpass : userpasses.entrySet()) {
                String response = ServerUtils.makeRequest(
                    Request.Get("http://localhost:9200"),
                    userpass.getKey(),
                    userpass.getValue(),
                    null
                );
                assertThat(response, containsString("You Know, for Search"));
            }
        });
    }

    public void test30AddBootstrapPassword() throws Exception {

        try (Stream<Path> dataFiles = Files.list(installation.data)) {
            // delete each dir under data, not data itself
            dataFiles.forEach(file -> {
                if (distribution.platform != Distribution.Platform.WINDOWS) {
                    FileUtils.rm(file);
                    return;
                }
                // HACK: windows asynchronously releases file locks after processes exit. Unfortunately there is no clear way to wait on
                // those locks being released. We might be able to use `openfiles /query`, but that requires modifying global settings
                // in our windows images with `openfiles /local on` (which requires a restart, thus needs to be baked into the images).
                // The following sleep allows time for windows to release the data file locks from Elasticsearch which was stopped in the
                // previous test.
                int retries = 30;
                Exception failure = null;
                while (retries-- > 0) {
                    try {
                        FileUtils.rm(file);
                        return;
                    } catch (Exception e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                throw new RuntimeException("failed to delete " + file, failure);
            });
        }

        installation.executables().keystoreTool.run("add --stdin bootstrap.password", BOOTSTRAP_PASSWORD);

        assertWhileRunning(() -> {
            String response = ServerUtils.makeRequest(
                Request.Get("http://localhost:9200/_cluster/health?wait_for_status=green&timeout=180s"),
                "elastic",
                BOOTSTRAP_PASSWORD,
                null
            );
            assertThat(response, containsString("\"status\":\"green\""));
        });
    }

    public void test40GeneratePasswordsBootstrapAlreadySet() throws Exception {
        assertWhileRunning(() -> {

            Shell.Result result = installation.executables().setupPasswordsTool.run("auto --batch", null);
            Map<String, String> userpasses = parseUsersAndPasswords(result.stdout);
            assertThat(userpasses, hasKey("elastic"));
            for (Map.Entry<String, String> userpass : userpasses.entrySet()) {
                String response = ServerUtils.makeRequest(
                    Request.Get("http://localhost:9200"),
                    userpass.getKey(),
                    userpass.getValue(),
                    null
                );
                assertThat(response, containsString("You Know, for Search"));
            }
        });
    }

    private Map<String, String> parseUsersAndPasswords(String output) {
        Matcher matcher = USERPASS_REGEX.matcher(output);
        assertNotNull(matcher);
        Map<String, String> userpases = new HashMap<>();
        while (matcher.find()) {
            userpases.put(matcher.group(1), matcher.group(2));
        }
        return userpases;
    }
}
