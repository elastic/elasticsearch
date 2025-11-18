/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.admin.cluster.RestClusterRerouteAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;

/**
 * Test for serialization and parsing of {@link ClusterRerouteRequest} and its commands. See the superclass for, well, everything.
 */
public class ClusterRerouteRequestTests extends ESTestCase {
    private static final int ROUNDS = 30;
    private final ProjectId projectId = randomProjectIdOrDefault();
    private final List<Supplier<AllocationCommand>> RANDOM_COMMAND_GENERATORS = List.of(
        () -> new AllocateReplicaAllocationCommand(
            randomAlphaOfLengthBetween(2, 10),
            between(0, 1000),
            randomAlphaOfLengthBetween(2, 10),
            projectId
        ),
        () -> new AllocateEmptyPrimaryAllocationCommand(
            randomAlphaOfLengthBetween(2, 10),
            between(0, 1000),
            randomAlphaOfLengthBetween(2, 10),
            randomBoolean(),
            projectId
        ),
        () -> new AllocateStalePrimaryAllocationCommand(
            randomAlphaOfLengthBetween(2, 10),
            between(0, 1000),
            randomAlphaOfLengthBetween(2, 10),
            randomBoolean(),
            projectId
        ),
        () -> new CancelAllocationCommand(
            randomAlphaOfLengthBetween(2, 10),
            between(0, 1000),
            randomAlphaOfLengthBetween(2, 10),
            randomBoolean(),
            projectId
        ),
        () -> new MoveAllocationCommand(
            randomAlphaOfLengthBetween(2, 10),
            between(0, 1000),
            randomAlphaOfLengthBetween(2, 10),
            randomAlphaOfLengthBetween(2, 10),
            projectId
        )
    );
    private final NamedWriteableRegistry namedWriteableRegistry;

    public ClusterRerouteRequestTests() {
        namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
    }

    private ClusterRerouteRequest randomRequest() {
        ClusterRerouteRequest request = new ClusterRerouteRequest(randomTimeValue(), randomTimeValue());
        int commands = between(0, 10);
        for (int i = 0; i < commands; i++) {
            request.add(randomFrom(RANDOM_COMMAND_GENERATORS).get());
        }
        request.dryRun(randomBoolean());
        request.explain(randomBoolean());
        request.setRetryFailed(randomBoolean());
        return request;
    }

    public void testEqualsAndHashCode() {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            assertEquals(request, request);
            assertEquals(request.hashCode(), request.hashCode());

            ClusterRerouteRequest copy = new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(
                request.getCommands().commands().toArray(new AllocationCommand[0])
            );
            AcknowledgedRequest<ClusterRerouteRequest> clusterRerouteRequestAcknowledgedRequest = copy.dryRun(request.dryRun())
                .explain(request.explain());
            clusterRerouteRequestAcknowledgedRequest.ackTimeout(request.ackTimeout()).setRetryFailed(request.isRetryFailed());
            copy.masterNodeTimeout(request.masterNodeTimeout());
            assertEquals(request, copy);
            assertEquals(copy, request); // Commutative
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing dryRun makes requests not equal
            copy.dryRun(copy.dryRun() == false);
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.dryRun(copy.dryRun() == false);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing explain makes requests not equal
            copy.explain(copy.explain() == false);
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.explain(copy.explain() == false);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing timeout makes requests not equal
            copy.ackTimeout(timeValueMillis(request.ackTimeout().millis() + 1));
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.ackTimeout(request.ackTimeout());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing masterNodeTime makes requests not equal
            copy.masterNodeTimeout(timeValueMillis(request.masterNodeTimeout().millis() + 1));
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.masterNodeTimeout(request.masterNodeTimeout());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing commands makes requests not equal
            copy.add(randomFrom(RANDOM_COMMAND_GENERATORS).get());
            assertNotEquals(request, copy);
            // Can't check hashCode because we can't be sure that changing commands changes the hashCode. It usually does but might not.
        }
    }

    public void testSerialization() throws IOException {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            ClusterRerouteRequest copy = roundTripThroughBytes(request);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());
            assertNotSame(request, copy);
        }
    }

    public void testParsing() throws IOException {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            ClusterRerouteRequest copy = roundTripThroughRestRequest(request);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());
            assertNotSame(request, copy);
        }
    }

    private ClusterRerouteRequest roundTripThroughBytes(ClusterRerouteRequest original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return new ClusterRerouteRequest(in);
            }
        }
    }

    private ClusterRerouteRequest roundTripThroughRestRequest(ClusterRerouteRequest original) throws IOException {
        RestRequest restRequest = toRestRequest(original);
        return RestClusterRerouteAction.createRequest(restRequest, projectId);
    }

    private RestRequest toRestRequest(ClusterRerouteRequest original) throws IOException {
        Map<String, String> params = new HashMap<>();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        boolean hasBody = false;
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        if (randomBoolean()) {
            params.put("dry_run", Boolean.toString(original.dryRun()));
        } else {
            hasBody = true;
            builder.field("dry_run", original.dryRun());
        }
        params.put("explain", Boolean.toString(original.explain()));
        if (false == original.ackTimeout().equals(DEFAULT_ACK_TIMEOUT) || randomBoolean()) {
            params.put("timeout", original.ackTimeout().getStringRep());
        }
        if (original.isRetryFailed() || randomBoolean()) {
            params.put("retry_failed", Boolean.toString(original.isRetryFailed()));
        }
        if (false == original.masterNodeTimeout().equals(RestUtils.REST_MASTER_TIMEOUT_DEFAULT) || randomBoolean()) {
            params.put(REST_MASTER_TIMEOUT_PARAM, original.masterNodeTimeout().getStringRep());
        }
        if (original.getCommands() != null) {
            hasBody = true;
            original.getCommands().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();

        FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        requestBuilder.withParams(params);
        if (hasBody) {
            requestBuilder.withContent(BytesReference.bytes(builder), builder.contentType());
        }
        return requestBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(NetworkModule.getNamedXContents());
    }
}
