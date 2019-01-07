/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RoleMappingFileBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    private static final String ROLE_MAPPING_FILE_SETTING = DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING.getKey();

    protected Settings settings;

    @Before
    public void init() throws IOException {
        settings = Settings.builder()
                .put("resource.reload.interval.high", "100ms")
                .put("path.home", createTempDir())
                .build();
    }

    public void testBootstrapCheckOfValidFile() {
        Path file = getDataPath("role_mapping.yml");
        Settings ldapSettings = Settings.builder()
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("ldap1", ldapSettings, settings, TestEnvironment.newEnvironment(settings),
                new ThreadContext(Settings.EMPTY));
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        assertFalse(check.check(createTestContext(settings, null)).isFailure());
    }

    public void testBootstrapCheckOfMissingFile() {
        final String fileName = randomAlphaOfLength(10);
        Path file = createTempDir().resolve(fileName);
        Settings ldapSettings = Settings.builder()
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("the-realm-name", ldapSettings, settings, TestEnvironment.newEnvironment(settings),
                new ThreadContext(Settings.EMPTY));
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString("the-realm-name"));
        assertThat(result.getMessage(), containsString(fileName));
        assertThat(result.getMessage(), containsString("does not exist"));
    }

    public void testBootstrapCheckWithInvalidYaml() throws IOException {
        Path file = createTempFile("", ".yml");
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("junk"), StandardCharsets.UTF_16);

        Settings ldapSettings = Settings.builder()
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("the-realm-name", ldapSettings, settings, TestEnvironment.newEnvironment(settings),
                new ThreadContext(Settings.EMPTY));
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString("the-realm-name"));
        assertThat(result.getMessage(), containsString(file.toString()));
        assertThat(result.getMessage(), containsString("could not read"));
    }

    public void testBootstrapCheckWithInvalidDn() throws IOException {
        Path file = createTempFile("", ".yml");
        // A DN must have at least 1 '=' symbol
        Files.write(file, Collections.singletonList("role: not-a-dn"));

        Settings ldapSettings = Settings.builder()
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("the-realm-name", ldapSettings, settings, TestEnvironment.newEnvironment(settings),
                new ThreadContext(Settings.EMPTY));
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString("the-realm-name"));
        assertThat(result.getMessage(), containsString(file.toString()));
        assertThat(result.getMessage(), containsString("invalid DN"));
        assertThat(result.getMessage(), containsString("not-a-dn"));
    }
}
