/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.ldap.LdapGroupToRoleMapper;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.ldap.LdapName;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class AbstractGroupToRoleMapperTests extends ElasticsearchTestCase {

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
    public void shutdown() {
        threadPool.shutdownNow();
    }

    protected abstract AbstractGroupToRoleMapper createMapper(Path file, ResourceWatcherService watcherService);

    @Test
    public void testMapper_ConfiguredWithUnreadableFile() throws Exception {
        Path file = newTempFile().toPath();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        AbstractGroupToRoleMapper mapper = createMapper(file, watcherService);
        assertThat(mapper.mappingsCount(), is(0));
    }

    @Test
    public void testMapper_AutoReload() throws Exception {
        Path roleMappingFile = Paths.get(AbstractGroupToRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        Path file = Files.createTempFile(null, ".yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        AbstractGroupToRoleMapper mapper = createMapper(file, watcherService);
        mapper.addListener(new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        Set<String> roles = mapper.mapRoles(ImmutableList.of("cn=shield,ou=marvel,o=superheros"));
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

        roles = mapper.mapRoles(ImmutableList.of("cn=fantastic_four,ou=marvel,o=superheros"));
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(1));
        assertThat(roles, contains("fantastic_four"));
    }

    @Test
    public void testMapper_AutoReload_WithParseFailures() throws Exception {
        Path roleMappingFile = Paths.get(AbstractGroupToRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        Path file = Files.createTempFile(null, ".yml");
        Files.copy(roleMappingFile, file, StandardCopyOption.REPLACE_EXISTING);

        final CountDownLatch latch = new CountDownLatch(1);

        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        AbstractGroupToRoleMapper mapper = createMapper(file, watcherService);
        mapper.addListener(new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        Set<String> roles = mapper.mapRoles(ImmutableList.of("cn=shield,ou=marvel,o=superheros"));
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
        Path file = Paths.get(AbstractGroupToRoleMapperTests.class.getResource("role_mapping.yml").toURI());
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<LdapName, Set<String>> mappings = LdapGroupToRoleMapper.parseFile(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), is(2));

        LdapName ldapName = new LdapName("cn=avengers,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(ldapName));
        Set<String> roles = mappings.get(ldapName);
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(2));
        assertThat(roles, containsInAnyOrder("shield", "avenger"));

        ldapName = new LdapName("cn=shield,ou=marvel,o=superheros");
        assertThat(mappings, hasKey(ldapName));
        roles = mappings.get(ldapName);
        assertThat(roles, notNullValue());
        assertThat(roles, hasSize(1));
        assertThat(roles, containsInAnyOrder("shield"));
    }

    @Test
    public void testParseFile_Empty() throws Exception {
        Path file = newTempFile().toPath();
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        ImmutableMap<LdapName, Set<String>> mappings = LdapGroupToRoleMapper.parseFile(file, logger, "_type", "_name");
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
        ImmutableMap<LdapName, Set<String>> mappings = LdapGroupToRoleMapper.parseFile(file, logger, "_type", "_name");
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
            LdapGroupToRoleMapper.parseFile(file, logger, "_type", "_name");
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
        ImmutableMap<LdapName, Set<String>> mappings = LdapGroupToRoleMapper.parseFileLenient(file, logger, "_type", "_name");
        assertThat(mappings, notNullValue());
        assertThat(mappings.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.ERROR);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("failed to parse role mappings file"));
    }
}
