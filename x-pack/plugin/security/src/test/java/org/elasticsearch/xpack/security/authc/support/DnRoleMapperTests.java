/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import com.unboundid.ldap.sdk.DN;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DnRoleMapperTests extends ESTestCase {

    private static final String[] STARK_GROUP_DNS = new String[] {
        // groups can be named by different attributes, depending on the directory,
        // we don't care what it is named by
        "cn=shield,ou=marvel,o=superheros",
        "cn=avengers,ou=marvel,o=superheros",
        "group=genius, dc=mit, dc=edu",
        "groupName = billionaire , ou = acme",
        "gid = playboy , dc = example , dc = com",
        "groupid=philanthropist,ou=groups,dc=unitedway,dc=org" };

    protected Settings settings;
    protected Environment env;
    protected ThreadPool threadPool;

    @Before
    public void init() throws IOException {
        settings = Settings.builder().put("resource.reload.interval.high", "100ms").put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(settings);
        if (Files.exists(env.configFile()) == false) {
            Files.createDirectory(env.configFile());
        }
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testMapper_ConfiguredWithUnreadableFile() throws Exception {
        Path file = createTempFile("", ".yml");
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = createMapper(file, watcherService);
            assertThat(mapper.mappingsCount(), is(0));
        }
    }

    public void testMapper_AutoReload() throws Exception {
        Path roleMappingFile = getDataPath("role_mapping.yml");
        Path file = env.configFile().resolve("test_role_mapping.yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = createMapper(file, watcherService);
            mapper.addListener(latch::countDown);

            Set<String> roles = mapper.resolveRoles("", Collections.singletonList("cn=shield,ou=marvel,o=superheros"));
            assertThat(roles, notNullValue());
            assertThat(roles.size(), is(1));
            assertThat(roles, contains("security"));

            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }

            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            if (latch.getCount() != 1) {
                fail("Listener should not be called as roles mapping is not changed.");
            }

            roles = mapper.resolveRoles("", Collections.singletonList("cn=shield,ou=marvel,o=superheros"));
            assertThat(roles, contains("security"));

            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("fantastic_four:\n").append("  - \"cn=fantastic_four,ou=marvel,o=superheros\"");
            }

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("Waited too long for the updated file to be picked up");
            }

            roles = mapper.resolveRoles("", Collections.singletonList("cn=fantastic_four,ou=marvel,o=superheros"));
            assertThat(roles, notNullValue());
            assertThat(roles.size(), is(1));
            assertThat(roles, contains("fantastic_four"));
        }
    }

    public void testMapper_AutoReload_WithParseFailures() throws Exception {
        Path roleMappingFile = getDataPath("role_mapping.yml");
        Path file = env.configFile().resolve("test_role_mapping.yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = createMapper(file, watcherService);
            mapper.addListener(latch::countDown);

            Set<String> roles = mapper.resolveRoles("", Collections.singletonList("cn=shield,ou=marvel,o=superheros"));
            assertThat(roles, notNullValue());
            assertThat(roles.size(), is(1));
            assertThat(roles, contains("security"));

            // now replacing the content of the users file with something that cannot be read
            Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("Waited too long for the updated file to be picked up");
            }

            assertThat(mapper.mappingsCount(), is(0));
        }
    }

    public void testMapperAutoReloadWithoutListener() throws Exception {
        Path roleMappingFile = getDataPath("role_mapping.yml");
        Path file = env.configFile().resolve("test_role_mapping.yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = createMapper(file, watcherService);
            Set<String> roles = mapper.resolveRoles("", Collections.singletonList("cn=shield,ou=marvel,o=superheros"));
            assertThat(roles, notNullValue());
            assertThat(roles.size(), is(1));
            assertThat(roles, contains("security"));

            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("fantastic_four:\n").append("  - \"cn=fantastic_four,ou=marvel,o=superheros\"");
            }

            assertBusy(() -> {
                Set<String> resolvedRoles = mapper.resolveRoles("", Collections.singletonList("cn=fantastic_four,ou=marvel,o=superheros"));
                assertThat(resolvedRoles, notNullValue());
                assertThat(resolvedRoles.size(), is(1));
                assertThat(resolvedRoles, contains("fantastic_four"));
            }, 2L, TimeUnit.SECONDS);
        }
    }

    public void testAddNullListener() throws Exception {
        Path file = env.configFile().resolve("test_role_mapping.yml");
        Files.write(file, Collections.singleton(""));
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = createMapper(file, watcherService);
            NullPointerException e = expectThrows(NullPointerException.class, () -> mapper.addListener(null));
            assertEquals("listener cannot be null", e.getMessage());
        }
    }

    public void testParseFile() throws Exception {
        Path file = getDataPath("role_mapping.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, List<String>> mappings = DnRoleMapper.parseFile(file, logger, "_type", "_name", false);
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), is(3));

        DN dn = new DN("cn=avengers,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(dn.toNormalizedString()));
        List<String> roles = mappings.get(dn.toNormalizedString());
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(2));
        assertThat(roles, containsInAnyOrder("security", "avenger"));

        dn = new DN("cn=shield,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(dn.toNormalizedString()));
        roles = mappings.get(dn.toNormalizedString());
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(1));
        assertThat(roles, contains("security"));

        dn = new DN("cn=Horatio Hornblower,ou=people,o=sevenSeas");
        assertThat(mappings, hasKey(dn.toNormalizedString()));
        roles = mappings.get(dn.toNormalizedString());
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(1));
        assertThat(roles, contains("avenger"));
    }

    public void testParseFile_Empty() throws Exception {
        Path file = createTempDir().resolve("foo.yaml");
        Files.createFile(file);
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        Map<String, List<String>> mappings = DnRoleMapper.parseFile(file, logger, "_type", "_name", false);
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
        List<String> events = CapturingLogger.output(logger.getName(), Level.DEBUG);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("[0] role mappings found"));
        events.clear();
    }

    public void testParseFile_WhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAlphaOfLength(10));
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, List<String>> mappings = DnRoleMapper.parseFile(file, logger, "_type", "_name", false);
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));

        final ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> { DnRoleMapper.parseFile(file, logger, "_type", "_name", true); }
        );
        assertThat(exception.getMessage(), containsString(file.toString()));
        assertThat(exception.getMessage(), containsString("does not exist"));
        assertThat(exception.getMessage(), containsString("_name"));
    }

    public void testParseFile_WhenCannotReadFile() throws Exception {
        Path file = createTempFile("", ".yml");
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        try {
            DnRoleMapper.parseFile(file, logger, "_type", "_name", false);
            fail("expected a parse failure");
        } catch (Exception e) {
            this.logger.info("expected", e);
        }
    }

    public void testParseFileLenient_WhenCannotReadFile() throws Exception {
        Path file = createTempFile("", ".yml");
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, List<String>> mappings = DnRoleMapper.parseFileLenient(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("failed to parse role mappings file"));
        events.clear();
    }

    public void testYaml() throws Exception {
        Path file = getDataPath("role_mapping.yml");
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ldap1");
        Settings ldapSettings = Settings.builder()
            .put(settings)
            .put(getFullSettingKey(realmIdentifier, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), file.toAbsolutePath())
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            ldapSettings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(Settings.EMPTY)
        );

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = new DnRoleMapper(config, watcherService);

            Set<String> roles = mapper.resolveRoles("", Arrays.asList(STARK_GROUP_DNS));

            // verify
            assertThat(roles, hasItems("security", "avenger"));
        }
    }

    public void testRelativeDN() {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ldap1");
        Settings ldapSettings = Settings.builder()
            .put(settings)
            .put(getFullSettingKey(realmIdentifier, DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING), true)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            ldapSettings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(Settings.EMPTY)
        );

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = new DnRoleMapper(config, watcherService);

            Set<String> roles = mapper.resolveRoles("", Arrays.asList(STARK_GROUP_DNS));
            assertThat(roles, hasItems("genius", "billionaire", "playboy", "philanthropist", "shield", "avengers"));
        }
    }

    public void testUserDNMapping() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ldap-userdn-role");
        Path file = getDataPath("role_mapping.yml");
        Settings ldapSettings = Settings.builder()
            .put(settings)
            .put(getFullSettingKey(realmIdentifier, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), file.toAbsolutePath())
            .put(getFullSettingKey(realmIdentifier, DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING), false)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(
            realmIdentifier,
            ldapSettings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(Settings.EMPTY)
        );

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            DnRoleMapper mapper = new DnRoleMapper(config, watcherService);

            Set<String> roles = mapper.resolveRoles("cn=Horatio Hornblower,ou=people,o=sevenSeas", Collections.emptyList());
            assertThat(roles, hasItem("avenger"));
        }
    }

    protected DnRoleMapper createMapper(Path file, ResourceWatcherService watcherService) {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("ldap", "ad-group-mapper-test");
        Settings mergedSettings = Settings.builder()
            .put(settings)
            .put(getFullSettingKey(identifier, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), file.toAbsolutePath())
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(identifier, mergedSettings, env, new ThreadContext(Settings.EMPTY));
        return new DnRoleMapper(config, watcherService);
    }
}
