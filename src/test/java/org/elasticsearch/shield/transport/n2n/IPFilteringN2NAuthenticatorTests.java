/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

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

    private IPFilteringN2NAuthenticator ipFilteringN2NAuthenticator;

    @Test
    public void testThatIpV4AddressesCanBeProcessed() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "127.0.0.1")
                .put("shield.transport.filter.deny", "10.0.0.0/8")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsDenied("10.2.3.4");
    }

    @Test
    public void testThatIpV6AddressesCanBeProcessed() throws Exception {
        // you have to use the shortest possible notation in order to match, so
        // 1234:0db8:85a3:0000:0000:8a2e:0370:7334 becomes 1234:db8:85a3:0:0:8a2e:370:7334
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "2001:0db8:1234::/48")
                .putArray("shield.transport.filter.deny", "1234:db8:85a3:0:0:8a2e:370:7334", "4321:db8:1234::/48")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("2001:0db8:1234:0000:0000:8a2e:0370:7334");
        assertAddressIsDenied("1234:0db8:85a3:0000:0000:8a2e:0370:7334");
        assertAddressIsDenied("4321:0db8:1234:0000:0000:8a2e:0370:7334");
    }

    @Test
    @Network // requires network for name resolution
    public void testThatHostnamesCanBeProcessed() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "127.0.0.1")
                .put("shield.transport.filter.deny", "*.google.com")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsDenied("8.8.8.8");
    }

    @Test
    public void testThatAnAllowAllAuthenticatorWorks() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "_all")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsAllowed("173.194.70.100");
    }

    @Test
    public void testThatProfilesAreSupported() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "localhost")
                .put("shield.transport.filter.deny", "_all")
                .put("transport.profiles.client.shield.filter.allow", "192.168.0.1")
                .put("transport.profiles.client.shield.filter.deny", "_all")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsDenied("192.168.0.1");
        assertAddressIsAllowedForProfile("client", "192.168.0.1");
        assertAddressIsDeniedForProfile("client", "192.168.0.2");
    }

    @Test
    public void testThatAllowWinsOverDeny() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "10.0.0.1")
                .put("shield.transport.filter.deny", "10.0.0.0/8")
                .build();
        ipFilteringN2NAuthenticator = new IPFilteringN2NAuthenticator(settings);

        assertAddressIsAllowed("10.0.0.1");
        assertAddressIsDenied("10.0.0.2");
    }

    private void assertAddressIsAllowedForProfile(String profile, String ... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = String.format(Locale.ROOT, "Expected address %s to be allowed", inetAddress);
            assertThat(message, ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, profile, InetAddresses.forString(inetAddress), 1024), is(true));
        }
    }

    private void assertAddressIsAllowed(String ... inetAddresses) {
        assertAddressIsAllowedForProfile("default", inetAddresses);
    }

    private void assertAddressIsDeniedForProfile(String profile, String ... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = String.format(Locale.ROOT, "Expected address %s to be denied", inetAddress);
            assertThat(message, ipFilteringN2NAuthenticator.authenticate(NULL_PRINCIPAL, profile, InetAddresses.forString(inetAddress), 1024), is(false));
        }
    }

    private void assertAddressIsDenied(String ... inetAddresses) {
        assertAddressIsDeniedForProfile("default", inetAddresses);
    }
}
