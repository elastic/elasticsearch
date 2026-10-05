/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Response;

public class FetchSearchShardInformationResponseSerializationTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomLong());
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return new Response(randomValueOtherThan(instance.getLastSearcherAcquiredTime(), () -> randomLong()));
    }
}
