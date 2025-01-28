/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class TracesApmIT extends ESRestTestCase {
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();
    final String traceIdValue = "0af7651916cd43dd8448eb211c80319c";
    final String traceParentValue = "00-" + traceIdValue + "-b7ad6b7169203331-01";

    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "false")
        .setting("telemetry.tracing.enabled", "true")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testApmIntegration() throws Exception {
        Set<Predicate<Map<String, Object>>> assertions = new HashSet<>(
            Set.of(allTrue(transactionValue("name", equalTo("GET /_nodes/stats")), transactionValue("trace_id", equalTo(traceIdValue))))
        );

        CountDownLatch finished = new CountDownLatch(1);

        // a consumer that will remove the assertions from a map once it matched
        Consumer<String> messageConsumer = (String message) -> {
            var apmMessage = parseMap(message);
            if (isTransactionTraceMessage(apmMessage)) {
                logger.info("Apm transaction message received: " + message);
                assertions.removeIf(e -> e.test(apmMessage));
            }

            if (assertions.isEmpty()) {
                finished.countDown();
            }
        };

        mockApmServer.addMessageConsumer(messageConsumer);

        Request nodeStatsRequest = new Request("GET", "/_nodes/stats");

        nodeStatsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue).build());

        client().performRequest(nodeStatsRequest);

        finished.await(30, TimeUnit.SECONDS);
        assertThat(assertions, equalTo(Collections.emptySet()));
    }

    private boolean isTransactionTraceMessage(Map<String, Object> apmMessage) {
        return apmMessage.containsKey("transaction");
    }

    @SuppressWarnings("unchecked")
    private Predicate<Map<String, Object>> allTrue(Predicate<Map<String, Object>>... predicates) {
        var allTrueTest = Arrays.stream(predicates).reduce(v -> true, Predicate::and);
        return new Predicate<>() {
            @Override
            public boolean test(Map<String, Object> map) {
                return allTrueTest.test(map);
            }

            @Override
            public String toString() {
                return Arrays.stream(predicates).map(p -> p.toString()).collect(Collectors.joining(" and "));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Predicate<Map<String, Object>> transactionValue(String path, Matcher<T> expected) {

        return new Predicate<>() {
            @Override
            public boolean test(Map<String, Object> map) {
                var transaction = (Map<String, Object>) map.get("transaction");
                var value = XContentMapValues.extractValue(path, transaction);
                return expected.matches((T) value);
            }

            @Override
            public String toString() {
                StringDescription matcherDescription = new StringDescription();
                expected.describeTo(matcherDescription);
                return path + " " + matcherDescription;
            }
        };
    }

    private Map<String, Object> parseMap(String message) {
        try (XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, message)) {
            return parser.map();
        } catch (IOException e) {
            fail(e);
            return Collections.emptyMap();
        }
    }

}
