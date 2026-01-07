/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResultsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.InferenceFieldMetadataTests.generateRandomChunkingSettings;
import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction.GET_INFERENCE_FIELDS_ACTION_TV;

public class GetInferenceFieldsActionResponseTests extends AbstractBWCWireSerializationTestCase<GetInferenceFieldsAction.Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<GetInferenceFieldsAction.Response> instanceReader() {
        return GetInferenceFieldsAction.Response::new;
    }

    @Override
    protected GetInferenceFieldsAction.Response createTestInstance() {
        return new GetInferenceFieldsAction.Response(randomInferenceFieldsMap(), randomInferenceResultsMap());
    }

    @Override
    protected GetInferenceFieldsAction.Response mutateInstance(GetInferenceFieldsAction.Response instance) throws IOException {
        return switch (between(0, 1)) {
            case 0 -> new GetInferenceFieldsAction.Response(
                randomValueOtherThan(instance.getInferenceFieldsMap(), GetInferenceFieldsActionResponseTests::randomInferenceFieldsMap),
                instance.getInferenceResultsMap()
            );
            case 1 -> new GetInferenceFieldsAction.Response(
                instance.getInferenceFieldsMap(),
                randomValueOtherThan(instance.getInferenceResultsMap(), GetInferenceFieldsActionResponseTests::randomInferenceResultsMap)
            );
            default -> throw new AssertionError("Invalid value");
        };
    }

    private static Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> randomInferenceFieldsMap() {
        Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> map = new HashMap<>();
        int numIndices = randomIntBetween(0, 5);
        for (int i = 0; i < numIndices; i++) {
            String indexName = randomIdentifier();
            List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata> fields = new ArrayList<>();
            int numFields = randomIntBetween(0, 5);
            for (int j = 0; j < numFields; j++) {
                fields.add(randomeExtendedInferenceFieldMetadata());
            }
            map.put(indexName, fields);
        }
        return map;
    }

    @Override
    protected Collection<TransportVersion> bwcVersions() {
        TransportVersion minVersion = TransportVersion.max(TransportVersion.minimumCompatible(), GET_INFERENCE_FIELDS_ACTION_TV);
        return TransportVersionUtils.allReleasedVersions().tailSet(minVersion, true);
    }

    @Override
    protected GetInferenceFieldsAction.Response mutateInstanceForVersion(
        GetInferenceFieldsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static GetInferenceFieldsAction.ExtendedInferenceFieldMetadata randomeExtendedInferenceFieldMetadata() {
        return new GetInferenceFieldsAction.ExtendedInferenceFieldMetadata(randomInferenceFieldMetadata(), randomFloat());
    }

    private static InferenceFieldMetadata randomInferenceFieldMetadata() {
        return new InferenceFieldMetadata(
            randomIdentifier(),
            randomIdentifier(),
            randomIdentifier(),
            randomSet(1, 5, ESTestCase::randomIdentifier).toArray(String[]::new),
            generateRandomChunkingSettings()
        );
    }

    private static Map<String, InferenceResults> randomInferenceResultsMap() {
        Map<String, InferenceResults> map = new HashMap<>();
        int numResults = randomIntBetween(0, 5);
        for (int i = 0; i < numResults; i++) {
            String inferenceId = randomIdentifier();
            map.put(inferenceId, randomInferenceResults());
        }
        return map;
    }

    private static InferenceResults randomInferenceResults() {
        return randomFrom(
            MlDenseEmbeddingResultsTests.createRandomResults(),
            TextExpansionResultsTests.createRandomResults(),
            WarningInferenceResultsTests.createRandomResults(),
            ErrorInferenceResultsTests.createRandomResults()
        );
    }
}
