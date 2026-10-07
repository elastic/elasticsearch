/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Request;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.FETCH_SHARD_WARM_VOLUMES;
import static org.hamcrest.Matchers.equalTo;

public class FetchSearchShardInformationRequestSerializationTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomBoolean() ? null : randomIdentifier(), randomShardId(), randomBoolean());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new Request(
                randomValueOtherThan(instance.getNodeId(), () -> randomBoolean() ? null : randomIdentifier()),
                instance.getShardId(),
                instance.wantVolumes()
            );
            case 1 -> new Request(
                instance.getNodeId(),
                randomValueOtherThan(instance.getShardId(), FetchSearchShardInformationRequestSerializationTests::randomShardId),
                instance.wantVolumes()
            );
            case 2 -> new Request(instance.getNodeId(), instance.getShardId(), instance.wantVolumes() == false);
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testWantVolumesDroppedOnUnsupportedVersion() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(FETCH_SHARD_WARM_VOLUMES);
        final Request original = new Request(randomBoolean() ? null : randomIdentifier(), randomShardId(), true);
        final Request copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), version);
        assertThat(copy, equalTo(new Request(original.getNodeId(), original.getShardId(), false)));
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
