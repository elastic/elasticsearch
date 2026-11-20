/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportGetShardWarmingInfoAction.Response;

public class GetShardWarmingInfoResponseSerializationTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        if (rarely()) {
            return Response.NONE;
        }
        final long shardBytesToWarm = randomLongBetween(0, Long.MAX_VALUE / 2);
        return new Response(shardBytesToWarm, randomLongBetween(shardBytesToWarm, Long.MAX_VALUE));
    }

    @Override
    protected Response mutateInstance(Response instance) {
        if (instance.shardBytesToWarm() < instance.sourceBytesToWarm() && randomBoolean()) {
            final long shardBytesToWarm = randomValueOtherThan(
                instance.shardBytesToWarm(),
                () -> randomLongBetween(0, instance.sourceBytesToWarm())
            );
            return new Response(shardBytesToWarm, instance.sourceBytesToWarm());
        }
        final long sourceBytesToWarm = randomValueOtherThan(
            instance.sourceBytesToWarm(),
            () -> randomLongBetween(instance.shardBytesToWarm(), Long.MAX_VALUE)
        );
        return new Response(instance.shardBytesToWarm(), sourceBytesToWarm);
    }
}
