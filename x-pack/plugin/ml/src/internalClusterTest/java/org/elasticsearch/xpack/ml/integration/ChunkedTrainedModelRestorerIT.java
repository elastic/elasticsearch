/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;

import java.util.ArrayList;
import java.util.List;

public class ChunkedTrainedModelRestorerIT extends MlSingleNodeTestCase {

    private List<TrainedModelDefinitionDoc> createModelDefinitionDocs(List<String> compressedDefinitions, String modelId) {

        int totalLength = compressedDefinitions.stream().map(String::length).reduce(0, Integer::sum);

        List<TrainedModelDefinitionDoc> docs = new ArrayList<>();
        for (int i = 0; i < compressedDefinitions.size(); i++) {
            docs.add(new TrainedModelDefinitionDoc.Builder()
                .setDocNum(i)
                .setCompressedString(compressedDefinitions.get(i))
                .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                .setTotalDefinitionLength(totalLength)
                .setDefinitionLength(compressedDefinitions.get(i).length())
                .setEos(i == compressedDefinitions.size() - 1)
                .setModelId(modelId)
                .build());
        }

        return docs;
    }
}
