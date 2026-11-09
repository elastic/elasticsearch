/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;

public class TransportActionMetricsTests extends ESTestCase {

    private static final List<String> ALL_METRICS = List.of(
        "es.transport.sent.request.size",
        "es.transport.received.request.size",
        "es.transport.sent.response.size",
        "es.transport.received.response.size"
    );

    private RecordingMeterRegistry meterRegistry;
    private TransportActionMetrics metrics;

    @Before
    public void createMetrics() {
        meterRegistry = new RecordingMeterRegistry();
        metrics = new TransportActionMetrics(meterRegistry);
    }

    public void testRequestReceived() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onRequestReceived(randomNonNegativeLong(), action, size);

        assertOnlyRecorded("es.transport.received.request.size", measurement(size, action, false));
    }

    public void testResponseSent() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onResponseSent(randomNonNegativeLong(), action, size);

        assertOnlyRecorded("es.transport.sent.response.size", measurement(size, action, false));
    }

    public void testErrorResponseSentIsRecordedAsResponseSent() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onResponseSent(randomNonNegativeLong(), action, new IllegalStateException("simulated"), size);

        assertOnlyRecorded("es.transport.sent.response.size", measurement(size, action, false));
    }

    public void testRequestSent() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onRequestSent(randomNode(), randomNonNegativeLong(), action, randomRequest(), TransportRequestOptions.EMPTY, size);

        assertOnlyRecorded("es.transport.sent.request.size", measurement(size, action, false));
    }

    public void testResponseReceivedFromLocalCluster() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onResponseReceived(randomNonNegativeLong(), responseContext(action, mock(Transport.Connection.class)), size);

        assertOnlyRecorded("es.transport.received.response.size", measurement(size, action, false));
    }

    public void testResponseReceivedFromRemoteCluster() {
        final String action = randomAction();
        final int size = randomIntBetween(0, 10_000);

        metrics.onResponseReceived(randomNonNegativeLong(), responseContext(action, remoteConnection()), size);

        assertOnlyRecorded("es.transport.received.response.size", measurement(size, action, true));
    }

    public void testResponseReceivedWithoutContextIsIgnored() {
        // the context is missing when the request has already completed, e.g. because it timed out
        metrics.onResponseReceived(randomNonNegativeLong(), null, randomIntBetween(0, 10_000));

        assertNothingRecorded();
    }

    public void testMessagesWithoutNetworkMessageSizeAreIgnored() {
        // messages that are not sent over the network, e.g. requests the node sends to itself, have no size
        final int unavailable = -1;
        final String action = randomAction();

        metrics.onRequestReceived(randomNonNegativeLong(), action, unavailable);
        metrics.onResponseSent(randomNonNegativeLong(), action, unavailable);
        metrics.onResponseSent(randomNonNegativeLong(), action, new IllegalStateException("simulated"), unavailable);
        metrics.onRequestSent(randomNode(), randomNonNegativeLong(), action, randomRequest(), TransportRequestOptions.EMPTY, unavailable);
        metrics.onResponseReceived(randomNonNegativeLong(), responseContext(action, mock(Transport.Connection.class)), unavailable);

        assertNothingRecorded();
    }

    private static DiscoveryNode randomNode() {
        return DiscoveryNodeUtils.create(randomAlphaOfLength(5));
    }

    private static TestRequest randomRequest() {
        return new TestRequest(randomAlphaOfLength(5));
    }

    private static Transport.ResponseContext<TestResponse> responseContext(String action, Transport.Connection connection) {
        return new Transport.ResponseContext<>(
            new ActionListenerResponseHandler<>(ActionListener.noop(), TestResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE),
            connection,
            action,
            randomNonNegativeLong()
        );
    }

    private static Transport.Connection remoteConnection() {
        return RemoteConnectionManager.wrapConnectionWithRemoteInfo(
            mock(Transport.Connection.class),
            new RemoteConnectionManager.RemoteConnectionInfo(
                randomProjectIdOrDefault(),
                randomProjectIdOrDefault(),
                randomAlphaOfLength(5)
            ),
            mock(RemoteClusterCredentialsManager.class)
        );
    }

    private static String randomAction() {
        return "internal:test/" + randomAlphaOfLengthBetween(3, 10);
    }

    private void assertOnlyRecorded(String metric, Measurement expected) {
        for (String candidate : ALL_METRICS) {
            if (candidate.equals(metric)) {
                assertThat(candidate, measurements(candidate), contains(expected));
            } else {
                assertThat(candidate, measurements(candidate), empty());
            }
        }
    }

    private void assertNothingRecorded() {
        for (String metric : ALL_METRICS) {
            assertThat(metric, measurements(metric), empty());
        }
    }

    private List<Measurement> measurements(String metric) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, metric);
    }

    private static Measurement measurement(long bytes, String action, boolean isRemoteConnection) {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("es_transport_action_name", action);
        if (isRemoteConnection) {
            attributes.put("es_transport_connection_is_remote", true);
        }
        return new Measurement(bytes, attributes, false);
    }
}
