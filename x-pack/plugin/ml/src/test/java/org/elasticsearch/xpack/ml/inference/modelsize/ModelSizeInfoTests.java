/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModelSizeInfoTests extends AbstractXContentTestCase<ModelSizeInfo> {

    public static ModelSizeInfo createRandom() {
        return new ModelSizeInfo(EnsembleSizeInfoTests.createRandom(),
            randomBoolean() ?
                null :
                Stream.generate(() -> randomFrom(
                    FrequencyEncodingSizeTests.createRandom(),
                    OneHotEncodingSizeTests.createRandom(),
                    TargetMeanEncodingSizeTests.createRandom()))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList()));
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
        XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(FORMAT),
            XContentType.JSON);
        // Shouldn't throw
        doParseInstance(parser);
    }

    private static final String FORMAT = "" +
        "{\n" +
        "    \"trained_model_size\": {\n" +
        "        \"ensemble_model_size\": {\n" +
        "            \"tree_sizes\": [\n" +
        "                {\"num_nodes\": 7, \"num_leaves\": 8},\n" +
        "                {\"num_nodes\": 3, \"num_leaves\": 4},\n" +
        "                {\"num_leaves\": 1}\n" +
        "            ],\n" +
        "            \"feature_name_lengths\": [\n" +
        "                14,\n" +
        "                10,\n" +
        "                11\n" +
        "            ],\n" +
        "            \"num_output_processor_weights\": 3,\n" +
        "            \"num_classification_weights\": 0,\n" +
        "            \"num_classes\": 0,\n" +
        "            \"num_operations\": 3\n" +
        "        }\n" +
        "    },\n" +
        "    \"preprocessors\": [\n" +
        "        {\n" +
        "            \"one_hot_encoding\": {\n" +
        "                \"field_length\": 10,\n" +
        "                \"field_value_lengths\": [\n" +
        "                    10,\n" +
        "                    20\n" +
        "                ],\n" +
        "                \"feature_name_lengths\": [\n" +
        "                    15,\n" +
        "                    25\n" +
        "                ]\n" +
        "            }\n" +
        "        },\n" +
        "        {\n" +
        "            \"frequency_encoding\": {\n" +
        "                \"field_length\": 10,\n" +
        "                \"feature_name_length\": 5,\n" +
        "                \"field_value_lengths\": [\n" +
        "                    10,\n" +
        "                    20\n" +
        "                ]\n" +
        "            }\n" +
        "        },\n" +
        "        {\n" +
        "            \"target_mean_encoding\": {\n" +
        "                \"field_length\": 6,\n" +
        "                \"feature_name_length\": 15,\n" +
        "                \"field_value_lengths\": [\n" +
        "                    10,\n" +
        "                    20\n" +
        "                ]\n" +
        "            }\n" +
        "        }\n" +
        "    ]\n" +
        "} ";
}
