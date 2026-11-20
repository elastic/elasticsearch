/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.NO_SEARCHER_ACQUIRED_RESPONSE;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Response;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.SEARCH_SHARD_INFORMATION_ELAPSED_TIME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ShardInformationNodeResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return createLabeledTestInstance("1");
    }

    public static Response createLabeledTestInstance(String label) {
        return new Response(randomLong());
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return new Response(randomValueOtherThan(instance.getMillisSinceLastSearcherAcquired(), ESTestCase::randomLong));
    }

    public void testNegativeLongSerialization() throws IOException {
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            NO_SEARCHER_ACQUIRED_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
    }

    /**
     * An age must round-trip exactly, including zero, which is the value used for a freshly created write index.
     */
    public void testAgeRoundTripsAtCurrentVersion() throws IOException {
        final long age = randomLongBetween(0, TimeUnit.DAYS.toMillis(30));
        final Response response = copyInstance(new Response(age));
        assertThat(response.getMillisSinceLastSearcherAcquired(), equalTo(age));
    }

    /**
     * A node that predates {@link TransportFetchSearchShardInformationAction#SEARCH_SHARD_INFORMATION_ELAPSED_TIME} treats a non-negative
     * value as a timestamp of its own relative clock, so an age must not be sent to it.
     */
    public void testAgeIsNotSentToOlderNodes() throws IOException {
        final TransportVersion beforeElapsedTime = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ELAPSED_TIME);
        final Response response = copyInstance(new Response(randomLongBetween(0, Long.MAX_VALUE - 1)), beforeElapsedTime);
        // older nodes ignore values <= 0, which is what we want here rather than a bogus timestamp
        assertThat(response.getMillisSinceLastSearcherAcquired() <= 0, equalTo(true));
    }

    /**
     * A timestamp produced by an older node came from that node's relative clock and cannot be converted into an age here, so it must be
     * discarded rather than misinterpreted.
     */
    public void testTimestampFromOlderNodesIsDiscarded() throws IOException {
        final TransportVersion beforeElapsedTime = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ELAPSED_TIME);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(beforeElapsedTime);
            out.writeZLong(randomLongBetween(1, Long.MAX_VALUE - 1));
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(beforeElapsedTime);
                assertThat(new Response(in), equalTo(NO_SEARCHER_ACQUIRED_RESPONSE));
            }
        }
    }

    public void testSentinelsArePassedThroughToOlderNodes() throws IOException {
        final TransportVersion beforeElapsedTime = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ELAPSED_TIME);
        for (Response sentinel : new Response[] {
            NO_SEARCHER_ACQUIRED_RESPONSE,
            TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE,
            TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE }) {
            assertThat(copyInstance(sentinel, beforeElapsedTime), equalTo(sentinel));
        }
    }
}
