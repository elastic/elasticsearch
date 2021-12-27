/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class MlParserUtils {

    private MlParserUtils() {}

    /**
     * Parses an array of arrays of the given type
     *
     * @param fieldName the field name
     * @param valueParser the parser to use for the inner array values
     * @param parser the outer parser
     * @param <T> the type of the values of the inner array
     * @return a list of lists representing the array of arrays
     * @throws IOException an exception if parsing fails
     */
    public static <T> List<List<T>> parseArrayOfArrays(
        String fieldName,
        CheckedFunction<XContentParser, T, IOException> valueParser,
        XContentParser parser
    ) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
        }
        List<List<T>> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
            }
            List<T> innerList = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken().isValue() == false) {
                    throw new IllegalStateException(
                        "expected non-null value but got [" + parser.currentToken() + "] " + "for [" + fieldName + "]"
                    );
                }
                innerList.add(valueParser.apply(parser));
            }
            values.add(innerList);
        }
        return values;
    }

    /**
     * Parses a 3 dimensional array of doubles.
     *
     * @param fieldName the field name
     * @param parser the outer parser
     * @return The 3D array of doubles
     * @throws IOException If parsing fails
     */
    public static double[][][] parse3DArrayOfDoubles(String fieldName, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
        }
        List<List<List<Double>>> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
            }

            List<List<Double>> innerList = new ArrayList<>();

            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
                }

                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
                }

                List<Double> innerInner = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new IllegalStateException(
                            "expected non-null numerical value but got [" + parser.currentToken() + "] " + "for [" + fieldName + "]"
                        );
                    }
                    innerInner.add(parser.doubleValue());
                }
                innerList.add(innerInner);
            }
            values.add(innerList);
        }

        double[][][] val = new double[values.size()][values.get(0).size()][values.get(0).get(0).size()];

        for (int i = 0; i < val.length; i++) {
            for (int j = 0; j < val[0].length; j++) {
                double[] doubles = values.get(i).get(j).stream().mapToDouble(d -> d).toArray();
                System.arraycopy(doubles, 0, val[i][j], 0, doubles.length);
            }
        }

        return val;
    }
}
