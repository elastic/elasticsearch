/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResultsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChunkedInferenceActionResponseTests extends AbstractWireSerializingTestCase<ChunkedInferenceAction.Response> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<ChunkedInferenceAction.Response> instanceReader() {
        return ChunkedInferenceAction.Response::new;
    }

    @Override
    protected ChunkedInferenceAction.Response createTestInstance() {
        var inferences = new ArrayList<InferenceResults>();
        if (randomBoolean()) {
            inferences.add(ChunkedTextEmbeddingResultsTests.createRandomResults());
        } else {
            inferences.add(ChunkedTextExpansionResultsTests.createRandomResults());
        }
        return new ChunkedInferenceAction.Response(inferences);
    }

    @Override
    protected ChunkedInferenceAction.Response mutateInstance(ChunkedInferenceAction.Response instance) throws IOException {
        return null;
    }
}
