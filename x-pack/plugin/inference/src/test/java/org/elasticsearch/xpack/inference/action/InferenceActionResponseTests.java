/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.LegacyMlTextEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InferenceActionResponseTests extends AbstractBWCWireSerializationTestCase<InferenceAction.Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<InferenceAction.Response> instanceReader() {
        return InferenceAction.Response::new;
    }

    @Override
    protected InferenceAction.Response createTestInstance() {
        var result = switch (randomIntBetween(0, 2)) {
            case 0 -> TextEmbeddingFloatResultsTests.createRandomResults();
            case 1 -> LegacyMlTextEmbeddingResultsTests.createRandomResults().transformToTextEmbeddingResults();
            default -> SparseEmbeddingResultsTests.createRandomResults();
        };

        return new InferenceAction.Response(result);
    }

    @Override
    protected InferenceAction.Response mutateInstance(InferenceAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected InferenceAction.Response mutateInstanceForVersion(InferenceAction.Response instance, TransportVersion version) {
        return instance;
    }
}
