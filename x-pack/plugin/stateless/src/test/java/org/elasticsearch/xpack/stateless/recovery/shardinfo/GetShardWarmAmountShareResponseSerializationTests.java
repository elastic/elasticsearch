/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportGetShardWarmAmountShareAction.Response;

public class GetShardWarmAmountShareResponseSerializationTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        if (rarely()) {
            return Response.NO_SHARE;
        }
        final long shardAmount = randomLongBetween(0, Long.MAX_VALUE / 2);
        return new Response(shardAmount, randomLongBetween(shardAmount, Long.MAX_VALUE));
    }

    @Override
    protected Response mutateInstance(Response instance) {
        if (instance.shardAmount() < instance.competingSum() && randomBoolean()) {
            final long shardAmount = randomValueOtherThan(instance.shardAmount(), () -> randomLongBetween(0, instance.competingSum()));
            return new Response(shardAmount, instance.competingSum());
        }
        final long competingSum = randomValueOtherThan(
            instance.competingSum(),
            () -> randomLongBetween(instance.shardAmount(), Long.MAX_VALUE)
        );
        return new Response(instance.shardAmount(), competingSum);
    }
}
