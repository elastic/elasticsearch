/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.ClassRule;
import org.junit.Rule;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ApmIT extends ESRestTestCase {
    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .setting("tracing.apm.agent.metrics_interval", "1s")
        .setting("tracing.apm.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApmIntegration() throws Exception {
        Set<Predicate<RecordingApmServer.APMMessage>> assertions = new HashSet<>(
            Set.of(
                assertionWithDescription("testLongCounter", msg -> msg.getDouble("testLongCounter"), closeTo(1.0, 0.001)),
                assertionWithDescription("testDoubleCounter", msg -> msg.getDouble("testDoubleCounter"), closeTo(1.0, 0.001)),
                assertionWithDescription("testDoubleGauge", msg -> msg.getDouble("testDoubleGauge"), closeTo(1.0, 0.001)),
                assertionWithDescription("testLongGauge", msg -> msg.getLong("testLongGauge"), equalTo(1L)),
                assertionWithDescription(
                    "testDoubleHistogram",
                    msg -> msg.getHistogram("testDoubleHistogram").stream().mapToInt(RecordingApmServer.Bucket::count).sum(),
                    equalTo(2)
                ),
                assertionWithDescription(
                    "testLongHistogram",
                    msg -> msg.getHistogram("testLongHistogram").stream().mapToInt(RecordingApmServer.Bucket::count).sum(),
                    equalTo(2)
                )
            )
        );
        CountDownLatch finished = mockApmServer.addMessageAssertions(assertions);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        finished.await(30, TimeUnit.SECONDS);
        assertThat(mockApmServer.getMessageAssertions(), empty());
    }

    private <T> Predicate<RecordingApmServer.APMMessage> assertionWithDescription(
        String description,
        Function<RecordingApmServer.APMMessage, T> actual,
        Matcher<T> expected
    ) {
        return new Predicate<>() {
            @Override
            public boolean test(RecordingApmServer.APMMessage message) {
                return expected.matches(actual.apply(message));
            }

            @Override
            public String toString() {
                StringDescription matcherDescription = new StringDescription();
                expected.describeTo(matcherDescription);
                return description + " " + matcherDescription;
            }
        };
    }

}
