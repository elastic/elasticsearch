/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class TrainedModelDefinitionChunk implements ToXContentObject {

    private static final ParseField DEFINITION = new ParseField("definition");
    private static final ParseField TOTAL_DEFINITION_LENGTH = new ParseField("total_definition_length");

    public static final ConstructingObjectParser<TrainedModelDefinitionChunk, Void> PARSER = new ConstructingObjectParser<>(
        "chunked_trained_model_definition",
        a -> new TrainedModelDefinitionChunk((String) a[0], (Long) a[1]));

    static {
        PARSER.declareString(constructorArg(), DEFINITION);
        PARSER.declareLong(constructorArg(), TOTAL_DEFINITION_LENGTH);
    }

    private final String definition;
    private final long totalDefinitionLength;

    public TrainedModelDefinitionChunk(String definition, long totalDefinitionLength) {
        this.definition = definition;
        this.totalDefinitionLength = totalDefinitionLength;
    }

    public TrainedModelDefinitionDoc createTrainedModelDoc(String modelId, int docNum) {
        return new TrainedModelDefinitionDoc.Builder()
            .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
            .setTotalDefinitionLength(totalDefinitionLength)
            .setModelId(modelId)
            .setDefinitionLength(definition.length())
            .setDocNum(docNum)
            .setCompressedString(definition)
            .build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DEFINITION.getPreferredName(), definition);
        builder.field(TOTAL_DEFINITION_LENGTH.getPreferredName(), totalDefinitionLength);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDefinitionChunk that = (TrainedModelDefinitionChunk) o;
        return totalDefinitionLength == that.totalDefinitionLength &&
            Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition, totalDefinitionLength);
    }
}
