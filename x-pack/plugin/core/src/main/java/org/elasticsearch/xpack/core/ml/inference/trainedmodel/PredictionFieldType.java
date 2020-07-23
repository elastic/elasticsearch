/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * The type of the prediction field.
 * This modifies how the predicted class values are written for classification models
 */
public enum PredictionFieldType implements Writeable {

    STRING,
    NUMBER,
    BOOLEAN;

    private static final double EPS = 1.0E-9;

    public static PredictionFieldType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static PredictionFieldType fromStream(StreamInput in) throws IOException {
        return in.readEnum(PredictionFieldType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public Object transformPredictedValue(Double value, String stringRep) {
        if (value == null) {
            return null;
        }
        switch(this) {
            case STRING:
                return stringRep == null ? value.toString() : stringRep;
            case BOOLEAN:
                if ((areClose(value, 1.0D) || areClose(value, 0.0D)) == false) {
                    throw new IllegalArgumentException(
                        "Cannot transform numbers other than 0.0 or 1.0 to boolean. Provided number [" + value + "]");
                }
                return areClose(value, 1.0D);
            case NUMBER:
                if (Strings.isNullOrEmpty(stringRep)) {
                    return value;
                }
                // Quick check to verify that the string rep is LIKELY a number
                // Still handles the case where it throws and then returns the underlying value
                if (stringRep.charAt(0) == '-' || Character.isDigit(stringRep.charAt(0))) {
                    try {
                        return Long.parseLong(stringRep);
                    } catch (NumberFormatException nfe) {
                        return value;
                    }
                }
                return value;
            default:
                return value;
        }
    }

    private static boolean areClose(double value1, double value2) {
        return Math.abs(value1 - value2) < EPS;
    }
}
