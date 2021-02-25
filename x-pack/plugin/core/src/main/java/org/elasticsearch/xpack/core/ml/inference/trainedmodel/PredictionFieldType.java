/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
                if (isNumberQuickCheck(stringRep)) {
                    try {
                        // 1 is true, 0 is false
                        return Integer.parseInt(stringRep) == 1;
                    } catch (NumberFormatException nfe) {
                        // do nothing, allow fall through to final fromDouble
                    }
                } else if (isBoolQuickCheck(stringRep)) { // if we start with t/f case insensitive, it indicates boolean string
                    return Boolean.parseBoolean(stringRep);
                }
                return fromDouble(value);
            case NUMBER:
                // Quick check to verify that the string rep is LIKELY a number
                // Still handles the case where it throws and then returns the underlying value
                if (isNumberQuickCheck(stringRep)) {
                    try {
                        return Long.parseLong(stringRep);
                    } catch (NumberFormatException nfe) {
                        // do nothing, allow fall through to final return
                    }
                }
                return value;
            default:
                return value;
        }
    }

    private static boolean fromDouble(double value) {
        if ((areClose(value, 1.0D) || areClose(value, 0.0D)) == false) {
            throw new IllegalArgumentException(
                "Cannot transform numbers other than 0.0 or 1.0 to boolean. Provided number [" + value + "]");
        }
        return areClose(value, 1.0D);
    }

    private static boolean areClose(double value1, double value2) {
        return Math.abs(value1 - value2) < EPS;
    }

    private static boolean isNumberQuickCheck(String stringRep) {
        return Strings.isNullOrEmpty(stringRep) == false && (stringRep.charAt(0) == '-' || Character.isDigit(stringRep.charAt(0)));
    }

    private static boolean isBoolQuickCheck(String stringRep) {
        if (Strings.isNullOrEmpty(stringRep)) {
            return false;
        }
        char c = stringRep.charAt(0);
        return 't' == c || 'T' == c || 'f' == c || 'F' == c;
    }
}
