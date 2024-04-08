/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import joptsimple.OptionSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtilsForTesting;
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

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_ELASTIC_PASSWORD_HASH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;

public class AutoConfigGenerateElasticPasswordHashTests extends CommandTestCase {

    private static FileSystem jimfs;
    private static Hasher hasher;
    private Path confDir;
    private Settings settings;
    private Environment env;

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM, uses keystore that is not password protected", inFipsJvm());
    }

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

    @Override
    protected Command newCommand() {
        return new AutoConfigGenerateElasticPasswordHash() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
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
        assertThat(keyStoreWrapper.getSettingNames(), containsInAnyOrder(AUTOCONFIG_ELASTIC_PASSWORD_HASH.getKey(), "keystore.seed"));
    }

    public void testExistingKeystoreWithWrongPassword() throws Exception {
        KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.load(env.configFile());
        assertNotNull(keyStoreWrapper);
        keyStoreWrapper.decrypt(new char[0]);
        // set a random password so that we fail to decrypt it in GenerateElasticPasswordHash#execute
        keyStoreWrapper.save(env.configFile(), randomAlphaOfLength(16).toCharArray());
        UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e.getMessage(), equalTo("Failed to generate a password for the elastic user"));
        assertThat(terminal.getOutput(), is(emptyString()));
    }
}
