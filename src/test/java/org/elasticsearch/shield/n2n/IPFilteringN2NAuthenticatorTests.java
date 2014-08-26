/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import com.google.common.io.Files;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Locale;

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
    private Settings settings;
    private IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator;

    @Before
    public void init() throws Exception {
        configFile = temporaryFolder.newFile();
        Settings resourceWatcherServiceSettings = settingsBuilder().put("watcher.interval.medium", TimeValue.timeValueMillis(200)).build();
        resourceWatcherService = new ResourceWatcherService(resourceWatcherServiceSettings, new ThreadPool("resourceWatcher")).start();
        settings = settingsBuilder().put("shield.n2n.file", configFile.getPath()).build();
    }

    @After
    public void shutdown() {
        resourceWatcherService.stop();
    }

    @Test
    public void testThatIpV4AddressesCanBeProcessed() throws Exception {
        writeConfigFile("allow: 127.0.0.1\ndeny: 10.0.0.0/8");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");
        assertAddressIsDenied(ipFilteringN2NAuthenticator, "10.2.3.4");
    }

    @Test
    public void testThatIpV6AddressesCanBeProcessed() throws Exception {
        writeConfigFile("allow: 2001:0db8:1234::/48\ndeny: 1234:0db8:85a3:0000:0000:8a2e:0370:7334");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "2001:0db8:1234:0000:0000:8a2e:0370:7334");
        assertAddressIsDenied(ipFilteringN2NAuthenticator, "1234:0db8:85a3:0000:0000:8a2e:0370:7334");
    }

    @Test
    public void testThatHostnamesCanBeProcessed() throws Exception {
        writeConfigFile("allow: localhost\ndeny: '*.google.com'");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");
        assertAddressIsDenied(ipFilteringN2NAuthenticator, "173.194.70.100");
    }

    @Test
    public void testThatFileDeletionResultsInAllowingAll() throws Exception {
        writeConfigFile("allow: 127.0.0.1");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");

        configFile.delete();
        assertThat(configFile.exists(), is(false));

        sleep(250);
        assertAddressIsDenied(ipFilteringN2NAuthenticator, "127.0.0.1");
    }

    @Test
    public void testThatAnAllowAllAuthenticatorWorks() throws Exception {
        writeConfigFile("allow: all");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");
        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "173.194.70.100");
    }

    @Test
    public void testThatCommaSeparatedValuesWork() throws Exception {
        writeConfigFile("allow: 192.168.23.0/24, localhost\ndeny: all");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "192.168.23.1");
        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");
        assertAddressIsDenied(ipFilteringN2NAuthenticator, "10.1.2.3");
    }

    @Test
    public void testThatOrderIsImportant() throws Exception {
        writeConfigFile("deny: localhost\nallow: localhost");

        assertAddressIsDenied(ipFilteringN2NAuthenticator, "127.0.0.1");
    }

    @Test
    public void testThatOrderIsImportantViceVersa() throws Exception {
        writeConfigFile("allow: localhost\ndeny: localhost");

        assertAddressIsAllowed(ipFilteringN2NAuthenticator, "127.0.0.1");
    }

    @Test
    public void testThatEmptyFileDoesNotLeadIntoLoop() throws Exception {
        writeConfigFile("# \n\n");

        assertAddressIsDenied(ipFilteringN2NAuthenticator, "127.0.0.1");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testThatInvalidFileThrowsCorrectException() throws Exception {
        writeConfigFile("deny: all allow: all \n\n");
        IPFilteringN2NAuthenticator.parseFile(configFile.toPath());
    }

    private void writeConfigFile(String data) throws IOException {
        Files.write(data.getBytes(Charsets.UTF_8), configFile);
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings, new Environment(), resourceWatcherService);
    }

    private void assertAddressIsAllowed(IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator, String ... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = String.format(Locale.ROOT, "Expected address %s to be allowed", inetAddress);
            assertThat(message, ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString(inetAddress), 1024), is(true));
        }
    }

    private void assertAddressIsDenied(IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator, String ... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = String.format(Locale.ROOT, "Expected address %s to be denied", inetAddress);
            assertThat(message, ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, InetAddresses.forString(inetAddress), 1024), is(false));
        }
    }
}
