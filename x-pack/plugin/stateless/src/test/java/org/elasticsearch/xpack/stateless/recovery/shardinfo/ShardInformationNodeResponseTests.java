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

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ShardInformationNodeResponseTests extends AbstractWireSerializingTestCase<
    TransportFetchSearchShardInformationAction.Response> {

    @Override
    protected Writeable.Reader<TransportFetchSearchShardInformationAction.Response> instanceReader() {
        return TransportFetchSearchShardInformationAction.Response::new;
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response createTestInstance() {
        return createLabeledTestInstance("1");
    }

    public static TransportFetchSearchShardInformationAction.Response createLabeledTestInstance(String label) {
        return new TransportFetchSearchShardInformationAction.Response(randomLong());
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response mutateInstance(
        TransportFetchSearchShardInformationAction.Response instance
    ) {
        return new TransportFetchSearchShardInformationAction.Response(
            randomValueOtherThan(instance.getLastSearcherAcquiredTime(), ESTestCase::randomLong)
        );
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
    }

    /**
     * A node that predates {@link TransportFetchSearchShardInformationAction#SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME} treats a positive
     * value as a timestamp of its own relative clock, so an epoch timestamp must not be sent to it.
     */
    public void testTimestampIsNotSentToOlderNodes() throws IOException {
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(olderVersion);
            new TransportFetchSearchShardInformationAction.Response(randomLongBetween(1, Long.MAX_VALUE)).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(olderVersion);
                // read the raw value as older nodes do, they ignore values <= 0
                assertThat(in.readZLong(), equalTo(0L));
            }
        }
    }

    /**
     * A positive value from a node that predates {@link TransportFetchSearchShardInformationAction#SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME}
     * is a timestamp of that node's relative clock, so it must be discarded rather than taken as an epoch timestamp.
     */
    public void testTimestampFromOlderNodesIsDiscarded() throws IOException {
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(olderVersion);
            out.writeZLong(randomLongBetween(1, Long.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(olderVersion);
                assertThat(new TransportFetchSearchShardInformationAction.Response(in).getLastSearcherAcquiredTime(), equalTo(0L));
            }
        }
    }

    public void testNonPositiveValuesArePassedThroughToOlderNodes() throws IOException {
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_ABSOLUTE_TIME);
        for (TransportFetchSearchShardInformationAction.Response response : new TransportFetchSearchShardInformationAction.Response[] {
            new TransportFetchSearchShardInformationAction.Response(0),
            TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE,
            TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE }) {
            assertThat(copyInstance(response, olderVersion), equalTo(response));
        }
    }
}
