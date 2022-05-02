/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder<FieldValueFactorFunctionBuilder> {
    public static final String NAME = "field_value_factor";
    public static final FieldValueFactorFunction.Modifier DEFAULT_MODIFIER = FieldValueFactorFunction.Modifier.NONE;
    public static final float DEFAULT_FACTOR = 1;

    private final String field;
    private float factor = DEFAULT_FACTOR;
    private Double missing;
    private FieldValueFactorFunction.Modifier modifier = DEFAULT_MODIFIER;

    public FieldValueFactorFunctionBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field_value_factor: field must not be null");
        }
        this.field = fieldName;
    }

    /**
     * Read from a stream.
     */
    public FieldValueFactorFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        factor = in.readFloat();
        missing = in.readOptionalDouble();
        modifier = FieldValueFactorFunction.Modifier.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloat(factor);
        out.writeOptionalDouble(missing);
        modifier.writeTo(out);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String fieldName() {
        return this.field;
    }

    public FieldValueFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    public float factor() {
        return this.factor;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(double missing) {
        this.missing = missing;
        return this;
    }

    public Double missing() {
        return this.missing;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        if (modifier == null) {
            throw new IllegalArgumentException("field_value_factor: modifier must not be null");
        }
        this.modifier = modifier;
        return this;
    }

    public FieldValueFactorFunction.Modifier modifier() {
        return this.modifier;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        builder.field("factor", factor);
        if (missing != null) {
            builder.field("missing", missing);
        }
        builder.field("modifier", modifier.name().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    @Override
    protected boolean doEquals(FieldValueFactorFunctionBuilder functionBuilder) {
        return Objects.equals(this.field, functionBuilder.field)
            && Objects.equals(this.factor, functionBuilder.factor)
            && Objects.equals(this.missing, functionBuilder.missing)
            && Objects.equals(this.modifier, functionBuilder.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.factor, this.missing, this.modifier);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }

    @Override
    protected ScoreFunction doToFunction(SearchExecutionContext context) {
        IndexNumericFieldData fieldData = null;
        if (context.isFieldMapped(field)) {
            fieldData = context.getForField(context.getFieldType(field));
        } else {
            if (missing == null) {
                throw new ElasticsearchException("Unable to find a field mapper for field [" + field + "]. No 'missing' value defined.");
            }
        }
        return new FieldValueFactorFunction(field, factor, modifier, missing, fieldData);
    }

    public static FieldValueFactorFunctionBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        String currentFieldName = null;
        String field = null;
        float boostFactor = FieldValueFactorFunctionBuilder.DEFAULT_FACTOR;
        FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.NONE;
        Double missing = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("factor".equals(currentFieldName)) {
                    boostFactor = parser.floatValue();
                } else if ("modifier".equals(currentFieldName)) {
                    modifier = FieldValueFactorFunction.Modifier.fromString(parser.text());
                } else if ("missing".equals(currentFieldName)) {
                    missing = parser.doubleValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            } else if ("factor".equals(currentFieldName)
                && (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT)) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] field 'factor' does not support lists or objects"
                    );
                }
        }

        if (field == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] required field 'field' missing");
        }

        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = new FieldValueFactorFunctionBuilder(field).factor(boostFactor)
            .modifier(modifier);
        if (missing != null) {
            fieldValueFactorFunctionBuilder.missing(missing);
        }
        return fieldValueFactorFunctionBuilder;
    }
}
