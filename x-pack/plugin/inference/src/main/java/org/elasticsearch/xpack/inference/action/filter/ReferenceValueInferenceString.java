/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

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

class ReferenceValueInferenceString extends InferenceString {
    public static final String REFERENCE_VALUE_FIELD = "reference_value";

    static final ConstructingObjectParser<ReferenceValueInferenceString, Void> PARSER = new ConstructingObjectParser<>(
        ReferenceValueInferenceString.class.getSimpleName(),
        args -> new ReferenceValueInferenceString((DataType) args[0], (DataFormat) args[1], (String) args[2], (String) args[3])
    );
    static {
        InferenceString.declareCommonFields(PARSER);
        PARSER.declareString(optionalConstructorArg(), new ParseField(REFERENCE_VALUE_FIELD));
    }

    private final String referenceValue;

    ReferenceValueInferenceString(DataType dataType, @Nullable DataFormat dataFormat, String value, @Nullable String referenceValue) {
        super(dataType, dataFormat, value);
        this.referenceValue = referenceValue;
    }

    public String referenceValue() {
        return referenceValue;
    }

    @Override
    protected void validateWriteTo() {
        if (referenceValue != null) {
            throw new IllegalStateException("Cannot serialize a [" + REFERENCE_VALUE_FIELD + "] value");
        }
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (referenceValue != null) {
            builder.field(REFERENCE_VALUE_FIELD, referenceValue);
        }
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
