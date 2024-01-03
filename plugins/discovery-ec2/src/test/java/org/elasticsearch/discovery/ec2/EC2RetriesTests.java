/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.ec2.model.Instance;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "use a http server")
public class EC2RetriesTests extends AbstractEC2MockAPITestCase {

    @Override
    protected MockTransportService createTransportService() {
        return new MockTransportService(
            Settings.EMPTY,
            new Netty4Transport(
                Settings.EMPTY,
                TransportVersion.current(),
                threadPool,
                networkService,
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(Collections.emptyList()),
                new NoneCircuitBreakerService(),
                new SharedGroupFactory(Settings.EMPTY)
            ),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null
        );
    }

    public void testEC2DiscoveryRetriesOnRateLimiting() throws IOException {
        final String accessKey = "ec2_access";
        final List<String> hosts = List.of("127.0.0.1:9300");
        final Map<String, Integer> failedRequests = new ConcurrentHashMap<>();
        // retry the same request 5 times at most
        final int maxRetries = randomIntBetween(1, 5);
        httpServer.createContext("/", exchange -> {
            if (exchange.getRequestMethod().equals(HttpMethodName.POST.name())) {
                final String request = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
                final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
                if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {
                    final String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    if (auth == null || auth.contains(accessKey) == false) {
                        throw new IllegalArgumentException("wrong access key: " + auth);
                    }
                    if (failedRequests.compute(
                        exchange.getRequestHeaders().getFirst("Amz-sdk-invocation-id"),
                        (requestId, count) -> Objects.requireNonNullElse(count, 0) + 1
                    ) < maxRetries) {
                        exchange.sendResponseHeaders(HttpStatus.SC_SERVICE_UNAVAILABLE, -1);
                        return;
                    }
                    // Simulate an EC2 DescribeInstancesResponse
                    byte[] responseBody = null;
                    for (NameValuePair parse : URLEncodedUtils.parse(request, UTF_8)) {
                        if ("Action".equals(parse.getName())) {
                            responseBody = generateDescribeInstancesResponse(
                                hosts.stream().map(address -> new Instance().withPublicIpAddress(address)).collect(Collectors.toList())
                            );
                            break;
                        }
                    }
                    responseBody = responseBody == null ? new byte[0] : responseBody;
                    exchange.getResponseHeaders().set("Content-Type", "text/xml; charset=UTF-8");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, responseBody.length);
                    exchange.getResponseBody().write(responseBody);
                    return;
                }
            }
            fail("did not send response");
        });
        try (Ec2DiscoveryPlugin plugin = new Ec2DiscoveryPlugin(buildSettings(accessKey))) {
            final SeedHostsProvider seedHostsProvider = plugin.getSeedHostProviders(transportService, networkService).get("ec2").get();
            final SeedHostsResolver resolver = new SeedHostsResolver("test", Settings.EMPTY, transportService, seedHostsProvider);
            resolver.start();
            final List<TransportAddress> addressList = seedHostsProvider.getSeedAddresses(resolver);
            assertThat(addressList, Matchers.hasSize(1));
            assertThat(addressList.get(0).toString(), is(hosts.get(0)));
            assertThat(failedRequests, aMapWithSize(1));
            assertThat(failedRequests.values().iterator().next(), is(maxRetries));
        }
    }
}
