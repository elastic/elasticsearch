/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;

public class RenderQueryResponseSerializingTests extends AbstractWireSerializingTestCase<RenderSearchApplicationQueryAction.Response> {

    @Override
    protected Writeable.Reader<RenderSearchApplicationQueryAction.Response> instanceReader() {
        return RenderSearchApplicationQueryAction.Response::new;
    }

    @Override
    protected RenderSearchApplicationQueryAction.Response createTestInstance() {
        SearchSourceBuilder searchSourceBuilder = randomSearchSourceBuilder(
            () -> null,
            () -> null,
            null,
            Collections::emptyList,
            () -> null,
            () -> null
        );
        return new RenderSearchApplicationQueryAction.Response(searchSourceBuilder);
    }

    @Override
    protected RenderSearchApplicationQueryAction.Response mutateInstance(RenderSearchApplicationQueryAction.Response instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
