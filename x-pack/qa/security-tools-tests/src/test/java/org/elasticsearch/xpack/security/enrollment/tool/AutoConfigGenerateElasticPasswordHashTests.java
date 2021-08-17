/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_BOOOTSTRAP_ELASTIC_PASSWORD_HASH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;

public class AutoConfigGenerateElasticPasswordHashTests extends CommandTestCase {

    private static FileSystem jimfs;
    private static Hasher hasher;
    private Path confDir;
    private Settings settings;
    private Environment env;

    @BeforeClass
    public static void setupJimfs() {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        jimfs = Jimfs.newFileSystem(conf);
        PathUtilsForTesting.installMock(jimfs);
    }

    @Before
    public void setup() throws Exception {
        Path homeDir = jimfs.getPath("eshome");
        IOUtils.rm(homeDir);
        confDir = homeDir.resolve("config");
        Files.createDirectories(confDir);
        hasher = getFastStoredHashAlgoForTests();
        settings = Settings.builder()
            .put("path.home", homeDir)
            .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name())
            .build();
        env = new Environment(AutoConfigGenerateElasticPasswordHashTests.this.settings, confDir);
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.save(confDir, new char[0]);
    }

    @AfterClass
    public static void closeJimfs() throws IOException {
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    @Override protected Command newCommand() {
        return new AutoConfigGenerateElasticPasswordHash() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testSuccessfullyGenerateAndStoreHash() throws Exception {
        execute();
        assertThat(terminal.getOutput(), hasLength(20));
        KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.load(env.configFile());
        assertNotNull(keyStoreWrapper);
        keyStoreWrapper.decrypt(new char[0]);
        assertThat(keyStoreWrapper.getSettingNames(),
            containsInAnyOrder(AUTOCONFIG_BOOOTSTRAP_ELASTIC_PASSWORD_HASH.getKey(), "keystore.seed"));
    }

    public void testExistingKeystoreWithWrongPassword() throws Exception {
        KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.load(env.configFile());
        assertNotNull(keyStoreWrapper);
        keyStoreWrapper.decrypt(new char[0]);
        // set a random password so that we fail to decrypt it in GenerateElasticPasswordHash#execute
        keyStoreWrapper.save(env.configFile(), randomAlphaOfLength(8).toCharArray());
        execute();
        assertThat(terminal.getOutput(), is(emptyString()));
        assertThat(terminal.getErrorOutput(), containsString("Provided keystore password was incorrect"));
    }
}
