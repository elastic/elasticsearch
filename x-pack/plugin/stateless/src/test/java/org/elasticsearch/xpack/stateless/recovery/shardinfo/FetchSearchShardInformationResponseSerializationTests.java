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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Response;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.FETCH_SHARD_WARM_VOLUMES;
import static org.hamcrest.Matchers.equalTo;

public class FetchSearchShardInformationResponseSerializationTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        if (randomBoolean()) {
            return new Response(randomLong());
        }
        return new Response(randomLong(), randomIdentifier(), randomLong(), randomVolumes());
    }

    @Override
    protected Response mutateInstance(Response instance) {
        if (instance.volumesCollected() == false) {
            return switch (randomIntBetween(0, 1)) {
                case 0 -> new Response(randomValueOtherThan(instance.getLastSearcherAcquiredTime(), () -> randomLong()));
                case 1 -> new Response(instance.getLastSearcherAcquiredTime(), randomIdentifier(), randomLong(), randomVolumes());
                default -> throw new AssertionError("unreachable");
            };
        }
        return switch (randomIntBetween(0, 4)) {
            case 0 -> new Response(
                randomValueOtherThan(instance.getLastSearcherAcquiredTime(), () -> randomLong()),
                instance.respondingNodeId(),
                instance.volumesGeneration(),
                instance.volumes()
            );
            case 1 -> new Response(
                instance.getLastSearcherAcquiredTime(),
                randomValueOtherThan(instance.respondingNodeId(), () -> randomIdentifier()),
                instance.volumesGeneration(),
                instance.volumes()
            );
            case 2 -> new Response(
                instance.getLastSearcherAcquiredTime(),
                instance.respondingNodeId(),
                randomValueOtherThan(instance.volumesGeneration(), () -> randomLong()),
                instance.volumes()
            );
            case 3 -> new Response(
                instance.getLastSearcherAcquiredTime(),
                instance.respondingNodeId(),
                instance.volumesGeneration(),
                randomValueOtherThan(instance.volumes(), FetchSearchShardInformationResponseSerializationTests::randomVolumes)
            );
            case 4 -> new Response(instance.getLastSearcherAcquiredTime());
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testVolumesDroppedOnUnsupportedVersion() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(FETCH_SHARD_WARM_VOLUMES);
        final Response original = randomBoolean()
            ? new Response(randomLong())
            : new Response(randomLong(), randomIdentifier(), randomLong(), randomVolumes());
        final Response copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), version);
        assertThat(copy, equalTo(new Response(original.getLastSearcherAcquiredTime())));
    }

    private static Map<ShardId, Long> randomVolumes() {
        return randomMap(0, 5, () -> Tuple.tuple(randomShardId(), randomNonNegativeLong()));
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
