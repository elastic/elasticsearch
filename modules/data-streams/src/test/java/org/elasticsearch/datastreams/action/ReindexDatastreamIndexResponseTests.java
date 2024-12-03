/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.ReindexDataStreamIndexAction.Response;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ReindexDatastreamIndexResponseTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomAlphaOfLength(20));
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return new Response(randomValueOtherThan(instance.getDestIndex(), () -> randomAlphaOfLength(20)));
    }
}
