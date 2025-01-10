/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction.Request;

import java.util.EnumSet;

public class ReindexDatastreamIndexRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(20), randomAPIBlockSet());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return new ReindexDataStreamIndexAction.Request(
            randomValueOtherThan(instance.getSourceIndex(), () -> randomAlphaOfLength(20)),
            randomValueOtherThan(instance.getSourceBlocks(), this::randomAPIBlockSet)
        );
    }

    private EnumSet<IndexMetadata.APIBlock> randomAPIBlockSet() {
        var values = randomSet(0, 5, () -> randomFrom(IndexMetadata.APIBlock.values()));
        return values.isEmpty() ? EnumSet.noneOf(IndexMetadata.APIBlock.class) : EnumSet.copyOf(values);
    }
}
