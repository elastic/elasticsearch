/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModelSizeInfoTests extends AbstractXContentTestCase<ModelSizeInfo> {

    public static ModelSizeInfo createRandom() {
        return new ModelSizeInfo(
            EnsembleSizeInfoTests.createRandom(),
            randomBoolean()
                ? null
                : Stream.generate(
                    () -> randomFrom(
                        FrequencyEncodingSizeTests.createRandom(),
                        OneHotEncodingSizeTests.createRandom(),
                        TargetMeanEncodingSizeTests.createRandom()
                    )
                ).limit(randomIntBetween(1, 10)).collect(Collectors.toList())
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlModelSizeNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected ModelSizeInfo createTestInstance() {
        return createRandom();
    }

    @Override
    protected ModelSizeInfo doParseInstance(XContentParser parser) {
        return ModelSizeInfo.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testParseDescribedFormat() throws IOException {
        XContentParser parser = XContentHelper.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(FORMAT),
            XContentType.JSON
        );
        // Shouldn't throw
        doParseInstance(parser);
    }

    private static final String FORMAT = """
        {
            "trained_model_size": {
                "ensemble_model_size": {
                    "tree_sizes": [
                        {"num_nodes": 7, "num_leaves": 8},
                        {"num_nodes": 3, "num_leaves": 4},
                        {"num_leaves": 1}
                    ],
                    "feature_name_lengths": [
                        14,
                        10,
                        11
                    ],
                    "num_output_processor_weights": 3,
                    "num_classification_weights": 0,
                    "num_classes": 0,
                    "num_operations": 3
                }
            },
            "preprocessors": [
                {
                    "one_hot_encoding": {
                        "field_length": 10,
                        "field_value_lengths": [
                            10,
                            20
                        ],
                        "feature_name_lengths": [
                            15,
                            25
                        ]
                    }
                },
                {
                    "frequency_encoding": {
                        "field_length": 10,
                        "feature_name_length": 5,
                        "field_value_lengths": [
                            10,
                            20
                        ]
                    }
                },
                {
                    "target_mean_encoding": {
                        "field_length": 6,
                        "feature_name_length": 15,
                        "field_value_lengths": [
                            10,
                            20
                        ]
                    }
                }
            ]
        }\s""";
}
