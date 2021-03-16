/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FileServiceAccountsTokenStoreTests extends ESTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        final String hashingAlgorithm = inFipsJvm() ? randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_50000", "pbkdf2_stretch") :
            randomFrom("bcrypt", "bcrypt11", "pbkdf2", "pbkdf2_1000", "pbkdf2_50000", "pbkdf2_stretch");
        settings = Settings.builder()
            .put("resource.reload.interval.high", "100ms")
            .put("path.home", createTempDir())
            .put("xpack.security.authc.service_token_hashing.algorithm", hashingAlgorithm)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testParserFile() throws Exception {
        Path path = getDataPath("service_tokens");
        Map<String, char[]> parsedTokenHashes = FileServiceAccountsTokenStore.parseFile(path, null);
        assertThat(parsedTokenHashes, notNullValue());
        assertThat(parsedTokenHashes.size(), is(5));

        assertThat(new String(parsedTokenHashes.get("elastic/fleet/bcrypt")),
            equalTo("$2a$10$xxaI7z7eWdY6.7lgVQItw.MJt6I0da8gK6m7eABEUaAec/o7jGXu2"));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet/bcrypt10")),
            equalTo("$2a$10$2k4thZ7H8UOxVNHuDRpJP..w1gw1BqYeAcj6gZZ/nS2DCsozqPsFK"));

        assertThat(new String(parsedTokenHashes.get("elastic/fleet/pbkdf2")),
            equalTo("{PBKDF2}10000$vbR/3gubGOLdByccqLUIy3Xi1XMT1phPybckmZoA3XU=$EUXijIBSVxgtcCVbkktPV20j9scNQMGZ/OFu0z33qcM="));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet/pbkdf2_50000")),
            equalTo("{PBKDF2}50000$aHy+hWu92P2NURkqwRCtcK+UvIMoHYA3IMC9odX9OIg=$kS99n3wUGdqm54qOMqhdi6oSXXuwY/RV9IiShIQOpQk="));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet/pbkdf2_stretch")),
            equalTo("{PBKDF2_STRETCH}10000$2cFQnlHX6r0dpXNeQi2ZTCli/lk9ZHpiJygxXVcQZSo=$Uu0LT66DaWf8KCNmFdDjIojQtzsRf7232q17v1B8/y0="));

        assertThat(parsedTokenHashes.get("elastic/fleet/plain"), nullValue());
    }


    public void testAutoReload() {
        // TODO
    }

    public void testMalformattedFile() {
        // TODO
    }

}
