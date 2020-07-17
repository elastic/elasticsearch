/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RoleMappingFileBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    private static final RealmConfig.RealmIdentifier REALM_ID = new RealmConfig.RealmIdentifier("ldap", "ldap-realm-name");
    private static final String ROLE_MAPPING_FILE_SETTING = RealmSettings.getFullSettingKey(
            REALM_ID, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING);

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
                .put(settings)
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = getRealmConfig(ldapSettings);
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        assertFalse(check.check(createTestContext(settings, null)).isFailure());
    }

    private static RealmConfig getRealmConfig(Settings settings) {
        return new RealmConfig(REALM_ID,
            Settings.builder().put(settings).put(RealmSettings.getFullSettingKey(REALM_ID, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(settings), new ThreadContext(Settings.EMPTY));
    }

    public void testBootstrapCheckOfMissingFile() {
        final String fileName = randomAlphaOfLength(10);
        Path file = createTempDir().resolve(fileName);
        Settings ldapSettings = Settings.builder()
                .put(settings)
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = getRealmConfig(ldapSettings);
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString(REALM_ID.getName()));
        assertThat(result.getMessage(), containsString(fileName));
        assertThat(result.getMessage(), containsString("does not exist"));
    }

    public void testBootstrapCheckWithInvalidYaml() throws IOException {
        Path file = createTempFile("", ".yml");
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("junk"), StandardCharsets.UTF_16);

        Settings ldapSettings = Settings.builder()
                .put(settings)
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = getRealmConfig(ldapSettings);
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString(REALM_ID.getName()));
        assertThat(result.getMessage(), containsString(file.toString()));
        assertThat(result.getMessage(), containsString("could not read"));
    }

    public void testBootstrapCheckWithInvalidDn() throws IOException {
        Path file = createTempFile("", ".yml");
        // A DN must have at least 1 '=' symbol
        Files.write(file, Collections.singletonList("role: not-a-dn"));

        Settings ldapSettings = Settings.builder()
                .put(settings)
                .put(ROLE_MAPPING_FILE_SETTING, file.toAbsolutePath())
                .build();
        RealmConfig config = getRealmConfig(ldapSettings);
        final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(config);
        assertThat(check, notNullValue());
        assertThat(check.alwaysEnforce(), equalTo(true));
        final BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString(REALM_ID.getName()));
        assertThat(result.getMessage(), containsString(file.toString()));
        assertThat(result.getMessage(), containsString("invalid DN"));
        assertThat(result.getMessage(), containsString("not-a-dn"));
    }

}
