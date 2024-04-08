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
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.results.LegacyTextEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.TransportVersions.V_8_12_0;
import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Response.transformToServiceResults;

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
            case 0 -> TextEmbeddingResultsTests.createRandomResults();
            case 1 -> LegacyTextEmbeddingResultsTests.createRandomResults().transformToTextEmbeddingResults();
            default -> SparseEmbeddingResultsTests.createRandomResults();
        };

        return new InferenceAction.Response(result);
    }

    @Override
    protected InferenceAction.Response mutateInstance(InferenceAction.Response instance) throws IOException {
        return null;
    }

    @Override
    protected InferenceAction.Response mutateInstanceForVersion(InferenceAction.Response instance, TransportVersion version) {
        if (version.before(V_8_12_0)) {
            var singleResultList = instance.getResults().transformToLegacyFormat().subList(0, 1);
            return new InferenceAction.Response(transformToServiceResults(singleResultList));
        }

        return instance;
    }

    public void testSerializesInferenceServiceResultsAddedVersion() throws IOException {
        var instance = createTestInstance();
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    public void testSerializesOpenAiAddedVersion_UsingLegacyTextEmbeddingResult() throws IOException {
        var embeddingResults = LegacyTextEmbeddingResultsTests.createRandomResults().transformToTextEmbeddingResults();
        var instance = new InferenceAction.Response(embeddingResults);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    public void testSerializesOpenAiAddedVersion_UsingSparseEmbeddingResult() throws IOException {
        var embeddingResults = SparseEmbeddingResultsTests.createRandomResults();
        var instance = new InferenceAction.Response(embeddingResults);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    public void testSerializesMultipleInputsVersion_UsingLegacyTextEmbeddingResult() throws IOException {
        var embeddingResults = TextEmbeddingResultsTests.createRandomResults();
        var instance = new InferenceAction.Response(embeddingResults);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    public void testSerializesMultipleInputsVersion_UsingSparseEmbeddingResult() throws IOException {
        var embeddingResults = SparseEmbeddingResultsTests.createRandomResults();
        var instance = new InferenceAction.Response(embeddingResults);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    // Technically we should never see a text embedding result in the transport version of this test because support
    // for it wasn't added until openai
    public void testSerializesSingleInputVersion_UsingLegacyTextEmbeddingResult() throws IOException {
        var embeddingResults = TextEmbeddingResultsTests.createRandomResults();
        var instance = new InferenceAction.Response(embeddingResults);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }

    public void testSerializesSingleVersion_UsingSparseEmbeddingResult() throws IOException {
        var embeddingResults = SparseEmbeddingResultsTests.createRandomResults().transformToLegacyFormat().subList(0, 1);
        var instance = new InferenceAction.Response(transformToServiceResults(embeddingResults));
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), V_8_12_0);
        assertOnBWCObject(copy, instance, V_8_12_0);
    }
}
