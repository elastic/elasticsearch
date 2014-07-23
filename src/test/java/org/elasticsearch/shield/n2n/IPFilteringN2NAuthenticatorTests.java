/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.net.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.security.Principal;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class IPFilteringN2NAuthenticatorTests extends ElasticsearchTestCase {

    public static final Principal NULL_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "null";
        }
    };

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ResourceWatcherService resourceWatcherService;
    private File configFile;

    @Before
    public void init() throws Exception {
        configFile = temporaryFolder.newFile();
        Settings settings = settingsBuilder().put("watcher.interval.medium", TimeValue.timeValueMillis(200)).build();
        resourceWatcherService = new ResourceWatcherService(settings, new ThreadPool("resourceWatcher")).start();
    }

    @Test
    public void testThatIpV4AddressesCanBeProcessed() throws Exception {
        String testData = "allow: 127.0.0.1\ndeny: 10.0.0.0/8";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("10.2.3.4"), 1024), is(false));
    }

    @Test
    public void testThatIpV6AddressesCanBeProcessed() throws Exception {
        String testData = "allow: 2001:0db8:1234::/48\ndeny: 1234:0db8:85a3:0000:0000:8a2e:0370:7334";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("2001:0db8:1234:0000:0000:8a2e:0370:7334"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("1234:0db8:85a3:0000:0000:8a2e:0370:7334"), 1024), is(false));
    }

    @Test
    public void testThatHostnamesCanBeProcessed() throws Exception {
        String testData = "allow: localhost\ndeny: '*.google.com'";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("173.194.70.100"), 1024), is(false));
    }

    @Test
    public void testThatFileDeletionResultsInAllowingAll() throws Exception {
        String testData = "allow: 127.0.0.1";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));

        configFile.delete();
        assertThat(configFile.exists(), is(false));

        sleep(250);
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(false));
    }

    @Test
    public void testThatAnAllowAllAuthenticatorWorks() throws Exception {
        String testData = "allow: all";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("173.194.70.100"), 1024), is(true));
    }

    @Test
    public void testThatCommaSeparatedValuesWork() throws Exception {
        String testData = "allow: 192.168.23.0/24, localhost\ndeny: all";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("192.168.23.1"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));
        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("10.1.2.3"), 1024), is(false));
    }

    @Test
    public void testThatOrderIsImportant() throws Exception {
        String testData = "deny: localhost\nallow: localhost";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(false));
    }

    @Test
    public void testThatOrderIsImportantViceVersa() throws Exception {
        String testData = "allow: localhost\ndeny: localhost";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(true));
    }

    @Test
    public void testThatEmptyFileDoesNotLeadIntoLoop() throws Exception {
        String testData = "# \n\n";
        Files.write(testData.getBytes(Charsets.UTF_8), configFile);

        Settings settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
        IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);

        assertThat(ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString("127.0.0.1"), 1024), is(false));
    }
}
