/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Strings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.REMOTE_CLUSTER_FILTER_ALLOW_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.REMOTE_CLUSTER_FILTER_DENY_SETTING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IPFilterTests extends ESTestCase {
    private IPFilter ipFilter;
    private MockLicenseState licenseState;
    private AuditTrail auditTrail;
    private AuditTrailService auditTrailService;
    private Transport transport;
    private HttpServerTransport httpTransport;
    private ClusterSettings clusterSettings;

    @Before
    public void init() {
        licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(Security.IP_FILTERING_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        auditTrail = mock(AuditTrail.class);
        auditTrailService = new AuditTrailService(auditTrail, licenseState);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Stream.of(
                IPFilter.HTTP_FILTER_ALLOW_SETTING,
                IPFilter.HTTP_FILTER_DENY_SETTING,
                IPFilter.IP_FILTER_ENABLED_HTTP_SETTING,
                IPFilter.IP_FILTER_ENABLED_SETTING,
                IPFilter.TRANSPORT_FILTER_ALLOW_SETTING,
                IPFilter.TRANSPORT_FILTER_DENY_SETTING,
                TcpTransport.isUntrustedRemoteClusterEnabled() ? IPFilter.REMOTE_CLUSTER_FILTER_ALLOW_SETTING : null,
                TcpTransport.isUntrustedRemoteClusterEnabled() ? IPFilter.REMOTE_CLUSTER_FILTER_DENY_SETTING : null,
                IPFilter.PROFILE_FILTER_ALLOW_SETTING,
                IPFilter.PROFILE_FILTER_DENY_SETTING,
                TcpTransport.isUntrustedRemoteClusterEnabled() ? RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED : null
            ).filter(Objects::nonNull).collect(Collectors.toSet())
        );

        httpTransport = mock(HttpServerTransport.class);
        TransportAddress httpAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 9200);
        when(httpTransport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { httpAddress }, httpAddress));
        when(httpTransport.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        transport = mock(Transport.class);
        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { address }, address));
        when(transport.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        Map<String, BoundTransportAddress> profileBoundAddresses = new HashMap<>();
        profileBoundAddresses.put(
            "client",
            new BoundTransportAddress(new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9500) }, address)
        );

        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            profileBoundAddresses.put(
                REMOTE_CLUSTER_PROFILE,
                new BoundTransportAddress(new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9600) }, address)
            );
        }
        when(transport.profileBoundAddresses()).thenReturn(profileBoundAddresses);
    }

    public void testThatIpV4AddressesCanBeProcessed() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "127.0.0.1")
            .put("xpack.security.transport.filter.deny", "10.0.0.0/8")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsDenied("10.2.3.4");
    }

    public void testThatIpV6AddressesCanBeProcessed() throws Exception {
        // you have to use the shortest possible notation in order to match, so
        // 1234:0db8:85a3:0000:0000:8a2e:0370:7334 becomes 1234:db8:85a3:0:0:8a2e:370:7334
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "2001:0db8:1234::/48")
            .putList("xpack.security.transport.filter.deny", "1234:db8:85a3:0:0:8a2e:370:7334", "4321:db8:1234::/48")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());

        assertAddressIsAllowed("2001:0db8:1234:0000:0000:8a2e:0370:7334");
        assertAddressIsDenied("1234:0db8:85a3:0000:0000:8a2e:0370:7334");
        assertAddressIsDenied("4321:0db8:1234:0000:0000:8a2e:0370:7334");
    }

    @Network // requires network for name resolution
    public void testThatHostnamesCanBeProcessed() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.ml.autodetect_process", false)
            .put("xpack.security.transport.filter.allow", "127.0.0.1")
            .put("xpack.security.transport.filter.deny", "*.google.com")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());

        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsDenied("8.8.8.8");
    }

    public void testThatAnAllowAllAuthenticatorWorks() throws Exception {
        Settings settings = Settings.builder().put("xpack.security.transport.filter.allow", "_all").build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("127.0.0.1");
        assertAddressIsAllowed("173.194.70.100");
    }

    public void testThatProfilesAreSupported() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "localhost")
            .put("xpack.security.transport.filter.deny", "_all")
            .put("transport.profiles.client.xpack.security.filter.allow", "192.168.0.1")
            .put("transport.profiles.client.xpack.security.filter.deny", "_all")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("127.0.0.1");
        // when "localhost" is used, ES considers all local addresses see PatternRule#isLocalhost()
        assertAddressIsDenied(randomNonLocalIPv4Address());
        assertAddressIsAllowedForProfile("client", "192.168.0.1");
        assertAddressIsDeniedForProfile("client", "192.168.0.2");
    }

    public void testThatProfilesAreUpdateable() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "localhost")
            .put("xpack.security.transport.filter.deny", "_all")
            .put("transport.profiles.client.xpack.security.filter.allow", "192.168.0.1")
            .put("transport.profiles.client.xpack.security.filter.deny", "_all")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        Settings newSettings = Settings.builder()
            .putList("transport.profiles.client.xpack.security.filter.allow", "192.168.0.1", "192.168.0.2")
            .put("transport.profiles.client.xpack.security.filter.deny", "192.168.0.3")
            .build();
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, updatedSettingsBuilder, Settings.builder(), "test");
        clusterSettings.applySettings(updatedSettingsBuilder.build());
        assertAddressIsAllowed("127.0.0.1");
        // when "localhost" is used, ES considers all local addresses see PatternRule#isLocalhost()
        assertAddressIsDenied(randomNonLocalIPv4Address());
        assertAddressIsAllowedForProfile("client", "192.168.0.1", "192.168.0.2");
        assertAddressIsDeniedForProfile("client", "192.168.0.3");
    }

    public void testThatAllowWinsOverDeny() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "10.0.0.1")
            .put("xpack.security.transport.filter.deny", "10.0.0.0/8")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("10.0.0.1");
        assertAddressIsDenied("10.0.0.2");
    }

    public void testDefaultAllow() throws Exception {
        Settings settings = Settings.builder().build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("10.0.0.1");
        assertAddressIsAllowed("10.0.0.2");
    }

    public void testThatHttpWorks() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "127.0.0.1")
            .put("xpack.security.transport.filter.deny", "10.0.0.0/8")
            .put("xpack.security.http.filter.allow", "10.0.0.0/8")
            .put("xpack.security.http.filter.deny", "192.168.0.1")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundHttpTransportAddress(httpTransport.boundAddress());
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowedForProfile(IPFilter.HTTP_PROFILE_NAME, "10.2.3.4");
        assertAddressIsDeniedForProfile(IPFilter.HTTP_PROFILE_NAME, "192.168.0.1");
    }

    public void testThatHttpFallsbackToDefault() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "127.0.0.1")
            .put("xpack.security.transport.filter.deny", "10.0.0.0/8")
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundHttpTransportAddress(httpTransport.boundAddress());
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());

        assertAddressIsAllowedForProfile(IPFilter.HTTP_PROFILE_NAME, "127.0.0.1");
        assertAddressIsDeniedForProfile(IPFilter.HTTP_PROFILE_NAME, "10.2.3.4");
    }

    public void testThatBoundAddressIsNeverRejected() throws Exception {
        List<String> addressStrings = new ArrayList<>();
        for (TransportAddress address : transport.boundAddress().boundAddresses()) {
            addressStrings.add(NetworkAddress.format(address.address().getAddress()));
        }

        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                .putList("xpack.security.transport.filter.deny", addressStrings.toArray(new String[addressStrings.size()]))
                .build();
        } else {
            settings = Settings.builder().put("xpack.security.transport.filter.deny", "_all").build();
        }
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        ipFilter.setBoundHttpTransportAddress(httpTransport.boundAddress());

        for (String addressString : addressStrings) {
            assertAddressIsAllowedForProfile(IPFilter.HTTP_PROFILE_NAME, addressString);
            assertAddressIsAllowedForProfile("default", addressString);
        }
    }

    public void testThatAllAddressesAreAllowedWhenLicenseDisablesSecurity() {
        Settings settings = Settings.builder().put("xpack.security.transport.filter.deny", "_all").build();
        when(licenseState.isAllowed(Security.IP_FILTERING_FEATURE)).thenReturn(false);
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());

        // don't use the assert helper because we don't want the audit trail to be invoked here
        String message = Strings.format("Expected address %s to be allowed", "8.8.8.8");
        InetAddress address = InetAddresses.forString("8.8.8.8");
        assertThat(message, ipFilter.accept("default", new InetSocketAddress(address, 0)), is(true));
        verifyNoMoreInteractions(auditTrail);

        // for sanity enable license and check that it is denied
        when(licenseState.isAllowed(Security.IP_FILTERING_FEATURE)).thenReturn(true);
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());

        assertAddressIsDeniedForProfile("default", "8.8.8.8");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/62298")
    public void testThatNodeStartsWithIPFilterDisabled() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.filter.enabled", randomBoolean())
            .put("xpack.security.http.filter.enabled", randomBoolean())
            .build();
        try (Node node = new MockNode(settings, Arrays.asList(LocalStateSecurity.class))) {
            assertNotNull(node);
        }
    }

    public void testRemoteAccessCanBeFilteredSeparately() throws Exception {
        assumeTrue("tests Remote Cluster Security 2.0 functionality", TcpTransport.isUntrustedRemoteClusterEnabled());
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "192.168.0.2")
            .put("xpack.security.transport.filter.deny", "192.168.0.1")
            .put("xpack.security.remote_cluster.filter.allow", "192.168.0.1")
            .put("xpack.security.remote_cluster.filter.deny", "_all")
            .put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsAllowed("192.168.0.2");
        assertAddressIsDenied("192.168.0.1");
        assertAddressIsAllowedForProfile(REMOTE_CLUSTER_PROFILE, "192.168.0.1");
        assertAddressIsDeniedForProfile(REMOTE_CLUSTER_PROFILE, randomNonLocalIPv4Address());
    }

    /**
     * Checks that if the Remote Cluster port is enabled, it uses the IP filters configured for transport in general if the specific
     * remote_cluster filters are not set.
     */
    public void testThatRemoteAccessFallsBackToDefault() throws Exception {
        assumeTrue("tests Remote Cluster Security 2.0 functionality", TcpTransport.isUntrustedRemoteClusterEnabled());
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "192.168.0.1")
            .put("xpack.security.transport.filter.deny", "_all")
            .put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        assertAddressIsDenied(randomNonLocalIPv4Address());
        assertAddressIsAllowedForProfile(REMOTE_CLUSTER_PROFILE, "192.168.0.1");
        assertAddressIsDeniedForProfile(REMOTE_CLUSTER_PROFILE, randomNonLocalIPv4Address());
    }

    /**
     * This test is very similar to {@link #testThatProfilesAreUpdateable()}, but specifically checks the remote cluster port settings.
     */
    public void testThatRemoteAccessIsUpdateable() throws Exception {
        assumeTrue("tests Remote Cluster Security 2.0 functionality", TcpTransport.isUntrustedRemoteClusterEnabled());
        Settings settings = Settings.builder()
            .put("xpack.security.transport.filter.allow", "localhost")
            .put("xpack.security.transport.filter.deny", "_all")
            .put(REMOTE_CLUSTER_FILTER_ALLOW_SETTING.getKey(), "192.168.0.1")
            .put(REMOTE_CLUSTER_FILTER_DENY_SETTING.getKey(), "_all")
            .put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
            .build();
        ipFilter = new IPFilter(settings, auditTrailService, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        Settings newSettings = Settings.builder()
            .putList(REMOTE_CLUSTER_FILTER_ALLOW_SETTING.getKey(), "192.168.0.1", "192.168.0.2")
            .put(REMOTE_CLUSTER_FILTER_DENY_SETTING.getKey(), "192.168.0.3")
            .build();
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, updatedSettingsBuilder, Settings.builder(), "test");
        clusterSettings.applySettings(updatedSettingsBuilder.build());
        assertAddressIsAllowed("127.0.0.1");
        // when "localhost" is used, ES considers all local addresses see PatternRule#isLocalhost()
        assertAddressIsDenied(randomNonLocalIPv4Address());
        assertAddressIsAllowedForProfile(REMOTE_CLUSTER_PROFILE, "192.168.0.1", "192.168.0.2");
        assertAddressIsDeniedForProfile(REMOTE_CLUSTER_PROFILE, "192.168.0.3");
    }

    private void assertAddressIsAllowedForProfile(String profile, String... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = Strings.format("Expected address %s to be allowed", inetAddress);
            InetSocketAddress address = new InetSocketAddress(InetAddresses.forString(inetAddress), 0);
            assertTrue(message, ipFilter.accept(profile, address));
            ArgumentCaptor<SecurityIpFilterRule> ruleCaptor = ArgumentCaptor.forClass(SecurityIpFilterRule.class);
            verify(auditTrail).connectionGranted(eq(address), eq(profile), ruleCaptor.capture());
            assertNotNull(ruleCaptor.getValue());
        }
    }

    private void assertAddressIsAllowed(String... inetAddresses) {
        assertAddressIsAllowedForProfile("default", inetAddresses);
    }

    private void assertAddressIsDeniedForProfile(String profile, String... inetAddresses) {
        for (String inetAddress : inetAddresses) {
            String message = Strings.format("Expected address %s to be denied", inetAddress);
            InetSocketAddress address = new InetSocketAddress(InetAddresses.forString(inetAddress), 0);
            assertFalse(message, ipFilter.accept(profile, address));
            ArgumentCaptor<SecurityIpFilterRule> ruleCaptor = ArgumentCaptor.forClass(SecurityIpFilterRule.class);
            verify(auditTrail).connectionDenied(eq(address), eq(profile), ruleCaptor.capture());
            assertNotNull(ruleCaptor.getValue());
        }
    }

    private void assertAddressIsDenied(String... inetAddresses) {
        assertAddressIsDeniedForProfile("default", inetAddresses);
    }

    private String randomNonLocalIPv4Address() throws SocketException, UnknownHostException {
        String ipv4Address = null;
        int noOfRetries = 0;
        do {
            noOfRetries++;
            final InetAddress address = InetAddress.getByAddress(Numbers.intToBytes(randomInt()));
            if (address.isAnyLocalAddress() || address.isLoopbackAddress() || NetworkInterface.getByInetAddress(address) != null) {
                continue;
            } else {
                ipv4Address = NetworkAddress.format(address);
                break;
            }
        } while (ipv4Address == null && noOfRetries < 25);
        assertThat("could not generate random IPv4 address which is not local address", ipv4Address, notNullValue());
        return ipv4Address;
    }

}
