/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;

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
    public static <T> List<List<T>> parseArrayOfArrays(String fieldName, CheckedFunction<XContentParser, T, IOException> valueParser,
                                                       XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
        }
        List<List<T>> values = new ArrayList<>();
        while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
            }
            List<T> innerList = new ArrayList<>();
            while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if(parser.currentToken().isValue() == false) {
                    throw new IllegalStateException("expected non-null value but got [" + parser.currentToken() + "] " +
                        "for [" + fieldName + "]");
                }
                innerList.add(valueParser.apply(parser));
            }
            values.add(innerList);
        }
        return values;
    }
}
