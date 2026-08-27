/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
}
