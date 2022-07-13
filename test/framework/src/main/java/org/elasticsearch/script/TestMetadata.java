/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link Metadata} with customizable {@link org.elasticsearch.script.Metadata.Validator}s for use in testing.
 */
public class TestMetadata extends Metadata {
    public TestMetadata(Map<String, Object> map, Map<String, Validator> validators) {
        super(map, null, validators);
    }

    public static TestMetadata withNullableVersion(Map<String, Object> map) {
        Map<String, Validator> updatedValidators = new HashMap<>(VALIDATORS);
        updatedValidators.replace(VERSION, Metadata::longValidator);
        return new TestMetadata(map, updatedValidators);
    }
}
