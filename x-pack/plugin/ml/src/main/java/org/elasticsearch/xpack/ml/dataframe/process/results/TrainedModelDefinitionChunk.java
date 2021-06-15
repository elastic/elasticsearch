/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TrainedModelDefinitionChunk implements ToXContentObject {

    private static final ParseField DEFINITION = new ParseField("definition");
    private static final ParseField DOC_NUM = new ParseField("doc_num");
    private static final ParseField EOS = new ParseField("eos");

    public static final ConstructingObjectParser<TrainedModelDefinitionChunk, Void> PARSER = new ConstructingObjectParser<>(
        "chunked_trained_model_definition",
        a -> new TrainedModelDefinitionChunk((String) a[0], (Integer) a[1], (Boolean) a[2]));

    static {
        PARSER.declareString(constructorArg(), DEFINITION);
        PARSER.declareInt(constructorArg(), DOC_NUM);
        PARSER.declareBoolean(optionalConstructorArg(), EOS);
    }

    private final String definition;
    private final int docNum;
    private final Boolean eos;

    public TrainedModelDefinitionChunk(String definition, int docNum, Boolean eos) {
        this.definition = definition;
        this.docNum = docNum;
        this.eos = eos;
    }

    public TrainedModelDefinitionDoc createTrainedModelDoc(String modelId) {
        return new TrainedModelDefinitionDoc.Builder()
            .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
            .setModelId(modelId)
            .setDefinitionLength(definition.length())
            .setDocNum(docNum)
            .setCompressedString(definition)
            .setEos(isEos())
            .build();
    }

    public boolean isEos() {
        return eos != null && eos;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DEFINITION.getPreferredName(), definition);
        builder.field(DOC_NUM.getPreferredName(), docNum);
        if (eos != null) {
            builder.field(EOS.getPreferredName(), eos);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDefinitionChunk that = (TrainedModelDefinitionChunk) o;
        return docNum == that.docNum
            && Objects.equals(definition, that.definition)
            && Objects.equals(eos, that.eos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition, docNum, eos);
    }
}
