/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceState;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.Tag;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "use a http server")
public class Ec2DiscoveryTests extends AbstractEC2MockAPITestCase {

    private static final String SUFFIX_PRIVATE_DNS = ".ec2.internal";
    private static final String PREFIX_PRIVATE_DNS = "mock-ip-";
    private static final String SUFFIX_PUBLIC_DNS = ".amazon.com";
    private static final String PREFIX_PUBLIC_DNS = "mock-ec2-";
    private static final String PREFIX_PUBLIC_IP = "8.8.8.";
    private static final String PREFIX_PRIVATE_IP = "10.0.0.";

    private final Map<String, TransportAddress> poorMansDNS = new ConcurrentHashMap<>();

    protected MockTransportService createTransportService() {
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            writableRegistry(),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {
            @Override
            public TransportAddress[] addressesFromString(String address) {
                // we just need to ensure we don't resolve DNS here
                return new TransportAddress[] { poorMansDNS.getOrDefault(address, buildNewFakeTransportAddress()) };
            }
        };
        return new MockTransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null,
            null,
            null
        );
    }

    protected List<TransportAddress> buildDynamicHosts(Settings nodeSettings, int nodes) {
        return buildDynamicHosts(nodeSettings, nodes, null);
    }

    protected List<TransportAddress> buildDynamicHosts(Settings nodeSettings, int nodes, List<List<Tag>> tagsList) {
        final String accessKey = "ec2_key";
        try (Ec2DiscoveryPlugin plugin = new Ec2DiscoveryPlugin(buildSettings(accessKey))) {
            AwsEc2SeedHostsProvider provider = new AwsEc2SeedHostsProvider(nodeSettings, transportService, plugin.ec2Service);
            httpServer.createContext("/", exchange -> {
                if (SdkHttpMethod.POST.name().equals(exchange.getRequestMethod())) {
                    final String request = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
                    final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
                    if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {
                        final String auth = exchange.getRequestHeaders().getFirst("Authorization");
                        if (auth == null || auth.contains(accessKey) == false) {
                            throw new IllegalArgumentException("wrong access key: " + auth);
                        }
                        // Simulate an EC2 DescribeInstancesResponse
                        final Map<String, List<String>> tagsIncluded = new HashMap<>();
                        final String[] params = request.split("&");
                        Arrays.stream(params).filter(entry -> entry.startsWith("Filter.") && entry.contains("=tag%3A")).forEach(entry -> {
                            final int startIndex = "Filter.".length();
                            final int filterId = Integer.parseInt(entry.substring(startIndex, entry.indexOf(".", startIndex)));
                            tagsIncluded.put(
                                entry.substring(entry.indexOf("=tag%3A") + "=tag%3A".length()),
                                Arrays.stream(params)
                                    .filter(param -> param.startsWith("Filter." + filterId + ".Value."))
                                    .map(param -> param.substring(param.indexOf("=") + 1))
                                    .collect(Collectors.toList())
                            );
                        });
                        final List<Instance> instances = IntStream.range(1, nodes + 1).mapToObj(node -> {
                            final String instanceId = "node" + node;
                            final Instance.Builder instanceBuilder = Instance.builder()
                                .instanceId(instanceId)
                                .state(InstanceState.builder().name(InstanceStateName.RUNNING).build())
                                .privateDnsName(PREFIX_PRIVATE_DNS + instanceId + SUFFIX_PRIVATE_DNS)
                                .publicDnsName(PREFIX_PUBLIC_DNS + instanceId + SUFFIX_PUBLIC_DNS)
                                .privateIpAddress(PREFIX_PRIVATE_IP + node)
                                .publicIpAddress(PREFIX_PUBLIC_IP + node);
                            if (tagsList != null) {
                                instanceBuilder.tags(tagsList.get(node - 1));
                            }
                            return instanceBuilder.build();
                        })
                            .filter(
                                instance -> tagsIncluded.entrySet()
                                    .stream()
                                    .allMatch(
                                        entry -> instance.tags()
                                            .stream()
                                            .filter(t -> t.key().equals(entry.getKey()))
                                            .map(Tag::value)
                                            .toList()
                                            .containsAll(entry.getValue())
                                    )
                            )
                            .collect(Collectors.toList());
                        for (NameValuePair parse : URLEncodedUtils.parse(request, UTF_8)) {
                            if ("Action".equals(parse.getName())) {
                                final byte[] responseBody = generateDescribeInstancesResponse(instances);
                                exchange.getResponseHeaders().set("Content-Type", "text/xml; charset=UTF-8");
                                exchange.sendResponseHeaders(HttpStatus.SC_OK, responseBody.length);
                                exchange.getResponseBody().write(responseBody);
                                exchange.getResponseBody().flush();
                                return;
                            }
                        }
                    }
                }
                fail("did not send response");
            });
            List<TransportAddress> dynamicHosts = provider.getSeedAddresses(null);
            logger.debug("--> addresses found: {}", dynamicHosts);
            return dynamicHosts;
        } catch (IOException e) {
            fail("Unexpected IOException");
            return null;
        }
    }

    public void testDefaultSettings() {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder().build();
        List<TransportAddress> discoveryNodes = buildDynamicHosts(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
    }

    public void testPrivateIp() {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            poorMansDNS.put(PREFIX_PRIVATE_IP + (i + 1), buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "private_ip").build();
        List<TransportAddress> transportAddresses = buildDynamicHosts(nodeSettings, nodes);
        assertThat(transportAddresses, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : transportAddresses) {
            TransportAddress expected = poorMansDNS.get(PREFIX_PRIVATE_IP + node++);
            assertEquals(address, expected);
        }
    }

    public void testPublicIp() {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            poorMansDNS.put(PREFIX_PUBLIC_IP + (i + 1), buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "public_ip").build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            TransportAddress expected = poorMansDNS.get(PREFIX_PUBLIC_IP + node++);
            assertEquals(address, expected);
        }
    }

    public void testPrivateDns() {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            String instanceId = "node" + (i + 1);
            poorMansDNS.put(PREFIX_PRIVATE_DNS + instanceId + SUFFIX_PRIVATE_DNS, buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "private_dns").build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            String instanceId = "node" + node++;
            TransportAddress expected = poorMansDNS.get(PREFIX_PRIVATE_DNS + instanceId + SUFFIX_PRIVATE_DNS);
            assertEquals(address, expected);
        }
    }

    public void testPublicDns() {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            String instanceId = "node" + (i + 1);
            poorMansDNS.put(PREFIX_PUBLIC_DNS + instanceId + SUFFIX_PUBLIC_DNS, buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "public_dns").build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            String instanceId = "node" + node++;
            TransportAddress expected = poorMansDNS.get(PREFIX_PUBLIC_DNS + instanceId + SUFFIX_PUBLIC_DNS);
            assertEquals(address, expected);
        }
    }

    public void testInvalidHostType() {
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "does_not_exist").build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { buildDynamicHosts(nodeSettings, 1); });
        assertThat(exception.getMessage(), containsString("does_not_exist is unknown for discovery.ec2.host_type"));
    }

    public void testFilterByTags() {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder().put(AwsEc2Service.TAG_SETTING.getKey() + "stage", "prod").build();

        int prodInstances = 0;
        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            if (randomBoolean()) {
                tags.add(tag("stage", "prod"));
                prodInstances++;
            } else {
                tags.add(tag("stage", "dev"));
            }
            tagsList.add(tags);
        }

        logger.info("started [{}] instances with [{}] stage=prod tag", nodes, prodInstances);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(prodInstances));
    }

    private static Tag tag(String key, String value) {
        return Tag.builder().key(key).value(value).build();
    }

    public void testFilterByMultipleTags() {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder().putList(AwsEc2Service.TAG_SETTING.getKey() + "stage", "prod", "preprod").build();

        int prodInstances = 0;
        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            if (randomBoolean()) {
                tags.add(tag("stage", "prod"));
                if (randomBoolean()) {
                    tags.add(tag("stage", "preprod"));
                    prodInstances++;
                }
            } else {
                tags.add(tag("stage", "dev"));
                if (randomBoolean()) {
                    tags.add(tag("stage", "preprod"));
                }
            }
            tagsList.add(tags);
        }

        logger.info("started [{}] instances with [{}] stage=prod tag", nodes, prodInstances);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(prodInstances));
    }

    public void testReadHostFromTag() throws UnknownHostException {
        int nodes = randomIntBetween(5, 10);

        String[] addresses = new String[nodes];

        for (int node = 0; node < nodes; node++) {
            addresses[node] = "192.168.0." + (node + 1);
            poorMansDNS.put("node" + (node + 1), new TransportAddress(InetAddress.getByName(addresses[node]), 9300));
        }

        Settings nodeSettings = Settings.builder().put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "tag:foo").build();

        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            tags.add(tag("foo", "node" + (node + 1)));
            tagsList.add(tags);
        }

        logger.info("started [{}] instances", nodes);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(nodes));
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            TransportAddress expected = poorMansDNS.get("node" + node++);
            assertEquals(address, expected);
        }
    }

    abstract static class DummyEc2SeedHostsProvider extends AwsEc2SeedHostsProvider {
        public int fetchCount = 0;

        DummyEc2SeedHostsProvider(Settings settings, TransportService transportService, AwsEc2Service service) {
            super(settings, transportService, service);
        }
    }

    public void testGetNodeListEmptyCache() {
        AwsEc2Service awsEc2Service = new AwsEc2ServiceImpl();
        DummyEc2SeedHostsProvider provider = new DummyEc2SeedHostsProvider(Settings.EMPTY, transportService, awsEc2Service) {
            @Override
            protected List<TransportAddress> fetchDynamicNodes() {
                fetchCount++;
                return new ArrayList<>();
            }
        };
        for (int i = 0; i < 3; i++) {
            provider.getSeedAddresses(null);
        }
        assertThat(provider.fetchCount, is(1));
    }
}
