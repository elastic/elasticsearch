/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.DN;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LdapRoleMapperTests extends ElasticsearchTestCase {

    private static final String[] STARK_GROUP_DNS = new String[] {
            //groups can be named by different attributes, depending on the directory,
            //we don't care what it is named by
            "cn=shield,ou=marvel,o=superheros",
            "cn=avengers,ou=marvel,o=superheros",
            "group=genius, dc=mit, dc=edu",
            "groupName = billionaire , ou = acme",
            "gid = playboy , dc = example , dc = com",
            "groupid=philanthropist,ou=groups,dc=unitedway,dc=org"
    };

    protected Settings settings;
    protected Environment env;
    protected ThreadPool threadPool;

    @Before
    public void init() {
        settings = ImmutableSettings.builder()
                .put("watcher.interval.high", "2s")
                .build();
        env = new Environment(settings);
        threadPool = new ThreadPool("test");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    @Test
    public void testMapper_ConfiguredWithUnreadableFile() throws Exception {
        Path file = newTempFile().toPath();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        LdapRoleMapper mapper = createMapper(file, watcherService);
        assertThat(mapper.mappingsCount(), is(0));
    }

    @Test
    public void testMapper_AutoReload() throws Exception {
        Path roleMappingFile = Paths.get(LdapRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        Path file = Files.createTempFile(null, ".yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        LdapRoleMapper mapper = createMapper(file, watcherService);
        mapper.addListener(new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        Set<String> roles = mapper.resolveRoles("", ImmutableList.of("cn=shield,ou=marvel,o=superheros"));
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(1));
        assertThat(roles, contains("shield"));

        watcherService.start();

        try (BufferedWriter writer = Files.newBufferedWriter(file, Charsets.UTF_8, StandardOpenOption.APPEND)) {
            writer.newLine();
            writer.append("fantastic_four:\n")
                    .append("  - \"cn=fantastic_four,ou=marvel,o=superheros\"");
        }

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        roles = mapper.resolveRoles("", ImmutableList.of("cn=fantastic_four,ou=marvel,o=superheros"));
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(1));
        assertThat(roles, contains("fantastic_four"));
    }

    @Test
    public void testMapper_AutoReload_WithParseFailures() throws Exception {
        Path roleMappingFile = Paths.get(LdapRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        Path file = Files.createTempFile(null, ".yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        LdapRoleMapper mapper = createMapper(file, watcherService);
        mapper.addListener(new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        Set<String> roles = mapper.resolveRoles("", ImmutableList.of("cn=shield,ou=marvel,o=superheros"));
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(1));
        assertThat(roles, contains("shield"));

        watcherService.start();

        // now replacing the content of the users file with something that cannot be read
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        assertThat(mapper.mappingsCount(), is(0));
    }

    @Test
    public void testParseFile() throws Exception {
        Path file = Paths.get(LdapRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<DN, Set<String>> mappings = LdapRoleMapper.parseFile(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), is(3));

        DN dn = new DN("cn=avengers,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(dn));
        Set<String> roles = mappings.get(dn);
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(2));
        assertThat(roles, containsInAnyOrder("shield", "avenger"));

        dn = new DN("cn=shield,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(dn));
        roles = mappings.get(dn);
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(1));
        assertThat(roles, contains("shield"));

        dn = new DN("cn=Horatio Hornblower,ou=people,o=sevenSeas");
        assertThat(mappings, hasKey(dn));
        roles = mappings.get(dn);
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(1));
        assertThat(roles, contains("avenger"));
    }

    @Test
    public void testParseFile_Empty() throws Exception {
        Path file = newTempFile().toPath();
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<DN, Set<String>> mappings = LdapRoleMapper.parseFile(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.WARN);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("no mappings found"));
    }

    @Test
    public void testParseFile_WhenFileDoesNotExist() throws Exception {
        Path file = new File(randomAsciiOfLength(10)).toPath();
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<DN, Set<String>> mappings = LdapRoleMapper.parseFile(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
    }

    @Test
    public void testParseFile_WhenCannotReadFile() throws Exception {
        Path file = newTempFile().toPath();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        try {
            LdapRoleMapper.parseFile(file, logger, "_type", "_name");
            fail("expected a parse failure");
        } catch (Exception e) {
            this.logger.info("expected", e);
        }
    }

    @Test
    public void testParseFileLenient_WhenCannotReadFile() throws Exception {
        Path file = newTempFile().toPath();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<DN, Set<String>> mappings = LdapRoleMapper.parseFileLenient(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.ERROR);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("failed to parse role mappings file"));
    }

    @Test
    public void testYaml() throws IOException {
        File file = this.getResource("role_mapping.yml");
        Settings ldapSettings = ImmutableSettings.settingsBuilder()
                .put(LdapRoleMapper.ROLE_MAPPING_FILE_SETTING, file.getCanonicalPath())
                .build();
        RealmConfig config = new RealmConfig("ldap1", ldapSettings);

        LdapRoleMapper mapper = new LdapRoleMapper(LdapRealm.TYPE, config, new ResourceWatcherService(settings, threadPool), null);

        Set<String> roles = mapper.resolveRoles("", Arrays.asList(STARK_GROUP_DNS));

        //verify
        assertThat(roles, hasItems("shield", "avenger"));
    }

    @Test
    public void testRelativeDN() {
        Settings ldapSettings = ImmutableSettings.builder()
                .put(LdapRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ldap1", ldapSettings);

        LdapRoleMapper mapper = new LdapRoleMapper(LdapRealm.TYPE, config, new ResourceWatcherService(settings, threadPool), null);

        Set<String> roles = mapper.resolveRoles("", Arrays.asList(STARK_GROUP_DNS));
        assertThat(roles, hasItems("genius", "billionaire", "playboy", "philanthropist", "shield", "avengers"));
    }

    @Test
    public void testUserDNMapping() throws Exception {
        File file = this.getResource("role_mapping.yml");
        Settings ldapSettings = ImmutableSettings.builder()
                .put(LdapRoleMapper.ROLE_MAPPING_FILE_SETTING, file.getCanonicalPath())
                .put(LdapRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, false)
                .build();
        RealmConfig config = new RealmConfig("ldap-userdn-role", ldapSettings);

        LdapRoleMapper mapper = new LdapRoleMapper(LdapRealm.TYPE, config, new ResourceWatcherService(settings, threadPool), null);

        Set<String> roles = mapper.resolveRoles("cn=Horatio Hornblower,ou=people,o=sevenSeas", Collections.<String>emptyList());
        assertThat(roles, hasItem("avenger"));
    }

    protected LdapRoleMapper createMapper(Path file, ResourceWatcherService watcherService) {
        Settings realmSettings = ImmutableSettings.builder()
                .put("files.role_mapping", file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("ad-group-mapper-test", realmSettings, settings, env);
        return new LdapRoleMapper(randomBoolean() ? ActiveDirectoryRealm.TYPE : LdapRealm.TYPE, config, watcherService, null);
    }
}
