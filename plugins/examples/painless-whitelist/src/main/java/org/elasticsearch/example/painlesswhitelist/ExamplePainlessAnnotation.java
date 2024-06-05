/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.painlesswhitelist;

public class ExamplePainlessAnnotation {

    public static final String NAME = "example_annotation";

    public int category;
    public String message;

    public ExamplePainlessAnnotation(int category, String message) {
        this.category = category;
        this.message = message;
    }

    public int getCategory() {
        return category;
    }

    public String getMessage() {
        return message;
    }
}
