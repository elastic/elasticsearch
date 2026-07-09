/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ReferenceValueInferenceString extends InferenceString {
    public static final String REFERENCE_VALUE_FIELD = "reference_value";

    public static final ConstructingObjectParser<InferenceString, Void> PARSER = new ConstructingObjectParser<>(
        ReferenceValueInferenceString.class.getSimpleName(),
        args -> {
            DataType dataType = (DataType) args[0];
            DataFormat dataFormat = (DataFormat) args[1];
            String value = (String) args[2];
            String referenceValue = (String) args[3];
            if (referenceValue != null) {
                return new ReferenceValueInferenceString(dataType, dataFormat, value, referenceValue);
            } else {
                return new InferenceString(dataType, dataFormat, value);
            }
        }
    );
    static {
        InferenceString.declareCommonFields(PARSER);
        PARSER.declareString(optionalConstructorArg(), new ParseField(REFERENCE_VALUE_FIELD));
    }

    private final String referenceValue;

    public ReferenceValueInferenceString(
        DataType dataType,
        @Nullable DataFormat dataFormat,
        String value,
        String referenceValue
    ) {
        super(dataType, dataFormat, value);
        this.referenceValue = referenceValue;
    }

    public InferenceString truncateReferenceValue() {
        return new InferenceString(dataType(), dataFormat(), value());
    }

    public InferenceString replaceValueWithReferenceValue() {
        return new InferenceString(dataType(), DataFormat.REFERENCE, referenceValue);
    }

    public String referenceValue() {
        return referenceValue;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new IllegalStateException("[" + getClass().getSimpleName() + "] cannot be serialized");
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(REFERENCE_VALUE_FIELD, referenceValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (super.equals(obj) == false) return false;
        var that = (ReferenceValueInferenceString) obj;
        return Objects.equals(referenceValue, that.referenceValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), referenceValue);
    }

    @Override
    public String toString() {
        return "ReferenceValueInferenceString[" + super.toString() + ", referenceValue=" + referenceValue + ']';
    }
}
