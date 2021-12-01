/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.painlesswhitelist;

public class ExampleWhitelistedInstance {
    private final int value;

    public ExampleWhitelistedInstance(int value) {
        this.value = value;
    }

    public int addValue(int valueToAdd) {
        return this.value + valueToAdd;
    }

    public int getValue() {
        return value;
    }
}
