/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.function.SpecialFunctionGenerator;
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
        "ip_prefix",
        "cidr_match",
        "case"
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

    /**
     * {@code cidr_match} must emit an {@code ip/prefix} literal for the block argument. The
     * generic composite builder would otherwise recurse for keyword and produce a field name.
     */
    public void testCidrMatchGeneratorUsesLiteralPrefix() {
        GenerativeFunctionSignature sig = new GenerativeFunctionSignature(
            List.of(new GenerativeFunctionParam("ip", "ip", false), new GenerativeFunctionParam("blockX", "keyword", false)),
            "boolean",
            true
        );
        SpecialFunctionGenerator.Recurser recurse = (type, cols, unmapped, depth) -> "leaf_" + type;
        SpecialFunctionGenerator generator = SpecialFunctionGeneratorRegistry.forFunction("cidr_match");
        assertNotNull(generator);
        String expr = generator.generate("cidr_match", sig, List.of(), false, 2, recurse);
        assertNotNull(expr);
        assertTrue("expected cidr_match to use the ip expression, got " + expr, expr.startsWith("cidr_match(leaf_ip, \""));
        assertTrue(
            "expected a quoted CIDR literal, got " + expr,
            expr.contains("\"10.0.0.0/8\"")
                || expr.contains("\"192.168.0.0/16\"")
                || expr.contains("\"172.16.0.0/12\"")
                || expr.contains("\"0.0.0.0/0\"")
        );
        assertFalse("CIDR position must not be a generated keyword field, got " + expr, expr.contains("leaf_keyword"));
    }

    /**
     * Reproduces the precondition of https://github.com/elastic/elasticsearch/pull/149752:
     * a CASE condition composed of two ANDed/ORed sub-conditions, with an explicit trailing
     * {@code null} else-value. Over enough iterations, the {@code case} special generator must
     * be able to produce both — see https://github.com/elastic/elasticsearch/issues/150055.
     */
    public void testCaseGeneratorCanProduceAndOrConditionAndNullElse() {
        GenerativeFunctionSignature sig = new GenerativeFunctionSignature(
            List.of(
                new GenerativeFunctionParam("condition", "boolean", false),
                new GenerativeFunctionParam("trueValue", "boolean", false),
                new GenerativeFunctionParam("elseValue", "boolean", true)
            ),
            "boolean",
            true
        );
        SpecialFunctionGenerator.Recurser recurse = (type, cols, unmapped, depth) -> "leaf_" + type;
        SpecialFunctionGenerator caseGenerator = SpecialFunctionGeneratorRegistry.forFunction("case");

        boolean sawAndOr = false;
        boolean sawNullElse = false;
        for (int i = 0; i < 200 && (sawAndOr == false || sawNullElse == false); i++) {
            String expr = caseGenerator.generate("case", sig, List.of(), false, 3, recurse);
            assertNotNull(expr);
            sawAndOr |= expr.contains(" AND ") || expr.contains(" OR ");
            sawNullElse |= expr.endsWith(", null)");
        }
        assertTrue("expected case() to eventually compose an AND/OR condition", sawAndOr);
        assertTrue("expected case() to eventually emit an explicit null else-value", sawNullElse);
    }
}
