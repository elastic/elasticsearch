/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.painlesswhitelist;

import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.Map;

public class ExampleWhitelistAnnotationParser implements WhitelistAnnotationParser {

    public static final ExampleWhitelistAnnotationParser INSTANCE = new ExampleWhitelistAnnotationParser();

    private ExampleWhitelistAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("expected exactly two arguments");
        }

        String categoryString = arguments.get("category");

        if (categoryString == null) {
            throw new IllegalArgumentException("expected category argument");
        }

        int category;

        try {
            category = Integer.parseInt(categoryString);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("expected category as an int, found [" + categoryString + "]", nfe);
        }

        String message = arguments.get("message");

        if (categoryString == null) {
            throw new IllegalArgumentException("expected message argument");
        }

        return new ExamplePainlessAnnotation(category, message);
    }
}
