/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Function metadata written to {@code META-INF/esql/functions/<className>.json} by the
 * annotation processor at compile time, capturing information that is not available at runtime.
 * Currently that is just the class-level Javadoc description (before any block tags).
 */
public record SavedFunctionInfo(String description) {
    private static final ConstructingObjectParser<SavedFunctionInfo, Void> PARSER = new ConstructingObjectParser<>(
        "saved_function_info",
        a -> new SavedFunctionInfo((String) a[0])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
    }

    /**
     * Loads the saved function info for the given function class from the classpath resource
     * written by the annotation processor, or returns {@code null} if no resource exists.
     */
    public static SavedFunctionInfo load(Class<?> functionClass) throws IOException {
        String resourcePath = "META-INF/esql/functions/" + functionClass.getName().replace('.', '/') + ".json";
        try (InputStream stream = functionClass.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                return null;
            }
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, stream)) {
                return PARSER.parse(parser, null);
            }
        }
    }
}
