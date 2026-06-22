/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.function.SpecialFunctionGeneratorRegistry;

import java.util.List;

public class SpecialFunctionGeneratorRegistryTests extends ESTestCase {

    private static final List<String> KNOWN_SPECIAL_FUNCTIONS = List.of(
        "space",
        "split",
        "hash",
        "date_diff",
        "date_extract",
        "date_unit_count",
        "date_format",
        "mv_slice",
        "mv_percentile",
        "round_to",
        "decay",
        "scalb",
        "pow",
        "ip_prefix"
    );

    public void testKnownFunctionsHaveGenerators() {
        for (String fn : KNOWN_SPECIAL_FUNCTIONS) {
            assertNotNull("expected a special generator for '" + fn + "'", SpecialFunctionGeneratorRegistry.forFunction(fn));
        }
    }

    public void testUnknownFunctionHasNoGenerator() {
        assertNull(SpecialFunctionGeneratorRegistry.forFunction("does_not_exist"));
        assertNull(SpecialFunctionGeneratorRegistry.forFunction("abs"));
        assertNull(SpecialFunctionGeneratorRegistry.forFunction("to_string"));
    }
}
