/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.command.RegisteredDomainFunctionBridge;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Reflection target for {@code DocsV3Support}'s Kibana output-block rendering for the
 * {@code REGISTERED_DOMAIN} command. This is not a test suite (no test methods, no assertions);
 * it lives under {@code src/test} because that's the natural place to regenerate Kibana docs
 * from, matching {@link CommandLicenseTests}.
 */
public class RegisteredDomainOutputFields {

    /**
     * Returns the full set of possible output fields and their types, keyed by field name and sorted alphabetically.
     * Used by DocsV3Support to render the Kibana command definition's output block, found via reflection on
     * {@code RegisteredDomainOutputFields}.
     */
    public static SortedMap<String, DataType> allOutputFieldTypes() {
        SortedMap<String, DataType> result = new TreeMap<>();
        for (var entry : RegisteredDomainFunctionBridge.getAllOutputFields().entrySet()) {
            result.put(entry.getKey(), DataType.fromJavaType(entry.getValue()));
        }
        return result;
    }
}
