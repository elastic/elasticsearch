/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction.Response;

public class ReindexDatastreamIndexResponseTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomAlphaOfLength(20), randomLong());
    }

    @Override
    protected Response mutateInstance(Response instance) {
        String destIndex = instance.getDestIndex();
        long numCreated = instance.getNumCreated();
        switch (between(0, 1)) {
            case 0 -> destIndex = randomValueOtherThan(destIndex, () -> randomAlphaOfLength(20));
            case 1 -> numCreated++;
        }
        return new Response(destIndex, numCreated);
    }
}
