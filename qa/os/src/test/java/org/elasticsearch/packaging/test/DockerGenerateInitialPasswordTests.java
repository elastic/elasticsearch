/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.junit.After;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.docker.Docker.removeContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

public class DockerGenerateInitialPasswordTests extends PackagingTestCase {

    private static final Pattern PASSWORD_REGEX = Pattern.compile("Password for the (\\w+) user is: (.+)$", Pattern.MULTILINE);

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only Docker", distribution().isDocker());
    }

    @After
    public void teardownTest() {
        removeContainer();
    }

    /**
     * Checks that the Docker image can be run, and that it passes various checks.
     */
    public void test010Install() {
        installation = runContainer(
            distribution(),
            builder().envVars(Map.of("ingest.geoip.downloader.enabled", "false", "ELASTIC_PASSWORD", PASSWORD))
        );
        verifyContainerInstallation(installation);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(installation);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    private Map<String, String> parseUsersAndPasswords(String output) {
        Matcher matcher = PASSWORD_REGEX.matcher(output);
        assertNotNull(matcher);
        Map<String, String> usersAndPasswords = new HashMap<>();
        while (matcher.find()) {
            usersAndPasswords.put(matcher.group(1), matcher.group(2));
        }
        return usersAndPasswords;
    }
}
