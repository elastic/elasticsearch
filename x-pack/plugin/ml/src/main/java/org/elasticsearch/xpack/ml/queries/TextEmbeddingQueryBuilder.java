/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class TextEmbeddingQueryBuilder extends AbstractQueryBuilder<TextEmbeddingQueryBuilder> {

    public static final String NAME = "text_embedding";

    public static final ParseField MODEL_TEXT = new ParseField("model_text");

    private static final ConstructingObjectParser<TextEmbeddingQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new TextEmbeddingQueryBuilder((String) args[0], (String) args[1], (String) args[2], (int) args[3], (int) args[4])
    );

    static {
        PARSER.declareString(constructorArg(), InferModelAction.Request.MODEL_ID);
        PARSER.declareString(constructorArg(), MODEL_TEXT);
        PARSER.declareString(constructorArg(), KnnSearchBuilder.FIELD_FIELD);
        PARSER.declareInt(constructorArg(), KnnSearchBuilder.K_FIELD);
        PARSER.declareInt(constructorArg(), KnnSearchBuilder.NUM_CANDS_FIELD);
        PARSER.declareFloat(AbstractQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareString(AbstractQueryBuilder::queryName, NAME_FIELD);
    }

    public static TextEmbeddingQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String modelText;
    private final String denseVectorField;
    private final int k;
    private final int numCands;

    public TextEmbeddingQueryBuilder(String modelId, String modelText, String denseVectorField, int k, int numCands) {
        this.modelId = modelId;
        this.modelText = modelText;
        this.denseVectorField = denseVectorField;
        this.k = k;
        this.numCands = numCands;
    }

    public TextEmbeddingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.modelId = in.readString();
        this.modelText = in.readString();
        this.denseVectorField = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_7_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(modelText);
        out.writeString(denseVectorField);
        out.writeVInt(k);
        out.writeVInt(numCands);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(InferModelAction.Request.MODEL_ID.getPreferredName(), modelId);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.field(KnnSearchBuilder.FIELD_FIELD.getPreferredName(), denseVectorField);
        builder.field(KnnSearchBuilder.K_FIELD.getPreferredName(), k);
        builder.field(KnnSearchBuilder.NUM_CANDS_FIELD.getPreferredName(), numCands);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return null;
    }

    @Override
    protected boolean doEquals(TextEmbeddingQueryBuilder other) {
        return Objects.equals(modelId, other.modelId)
            && Objects.equals(modelText, other.modelText)
            && Objects.equals(denseVectorField, other.denseVectorField)
            && k == other.k
            && numCands == other.numCands
            && Float.compare(boost, other.boost) == 0;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(modelId, modelText, denseVectorField, k, numCands, boost);
    }
}
