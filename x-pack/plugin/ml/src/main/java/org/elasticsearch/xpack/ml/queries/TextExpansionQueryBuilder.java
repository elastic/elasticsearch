/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TextExpansionQueryBuilder extends AbstractQueryBuilder<TextExpansionQueryBuilder> {

    public static final String NAME = "text_expansion";
    public static final ParseField MODEL_TEXT = new ParseField("model_text");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    private static final String DEFAULT_MODEL_ID = "slim";

    private final String fieldName;
    private final String modelText;
    private final String modelId;

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (modelText == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + MODEL_TEXT.getPreferredName() + " value");
        }

        this.fieldName = fieldName;
        this.modelText = modelText;
        this.modelId = modelId == null ? DEFAULT_MODEL_ID : modelId;
    }

    public TextExpansionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.modelText = in.readString();
        this.modelId = in.readString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_7_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(modelText);
        out.writeString(modelId);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.field(MODEL_ID.getPreferredName(), modelId);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return null;
    }

    @Override
    protected boolean doEquals(TextExpansionQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(modelText, other.modelText)
            && Objects.equals(modelId, other.modelId);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, modelText, modelId);
    }

    public static TextExpansionQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String modelText = null;
        String modelId = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MODEL_TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                            modelText = parser.text();
                        } else if (MODEL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                            modelId = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                modelText = parser.text();
            }
        }

        if (modelText == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        TextExpansionQueryBuilder queryBuilder = new TextExpansionQueryBuilder(fieldName, modelText, modelId);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
