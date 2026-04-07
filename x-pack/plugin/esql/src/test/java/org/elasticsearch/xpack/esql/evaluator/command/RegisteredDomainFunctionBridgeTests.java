/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.web.RegisteredDomain.DOMAIN;
import static org.elasticsearch.web.RegisteredDomain.REGISTERED_DOMAIN;
import static org.elasticsearch.web.RegisteredDomain.SUBDOMAIN;
import static org.elasticsearch.web.RegisteredDomain.eTLD;

public class RegisteredDomainFunctionBridgeTests extends AbstractCompoundOutputEvaluatorTests {

    private final Warnings WARNINGS = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new WarningSourceLocation() {
        @Override
        public int lineNumber() {
            return 1;
        }

        @Override
        public int columnNumber() {
            return 2;
        }

        @Override
        public String viewName() {
            return null;
        }

        @Override
        public String text() {
            return "invalid_input";
        }
    });

    @Override
    protected CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector(List<String> requestedFields) {
        return new RegisteredDomainFunctionBridge.RegisteredDomainCollectorImpl(requestedFields);
    }

    @Override
    protected String collectorSimpleName() {
        return RegisteredDomainFunctionBridge.RegisteredDomainCollectorImpl.class.getSimpleName();
    }

    @Override
    protected Map<String, Class<?>> getSupportedOutputFieldMappings() {
        return RegisteredDomainFunctionBridge.getAllOutputFields();
    }

    public void testFullOutput() {
        List<String> requestedFields = List.of(DOMAIN, REGISTERED_DOMAIN, eTLD, SUBDOMAIN);
        List<String> input = List.of("www.example.co.uk");
        List<?> expected = List.of("www.example.co.uk", "example.co.uk", "co.uk", "www");
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMultiValue() {
        List<String> requestedFields = List.of(DOMAIN, REGISTERED_DOMAIN);
        List<String> input = List.of("www.example.co.uk", "elastic.co", "sub.example.com");
        List<Object[]> expected = Collections.nCopies(requestedFields.size(), new Object[] { null });
        evaluateAndCompare(input, requestedFields, expected, WARNINGS);
        assertCriticalWarnings(
            "Line 1:2: evaluation of [invalid_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.lang.IllegalArgumentException: This command doesn't support multi-value input"
        );
    }

    public void testPartialFieldsRequested() {
        List<String> requestedFields = List.of(REGISTERED_DOMAIN, eTLD);
        List<String> input = List.of("www.example.co.uk");
        List<?> expected = List.of("example.co.uk", "co.uk");
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testInvalidInput() {
        List<String> requestedFields = List.of(DOMAIN, REGISTERED_DOMAIN);
        List<String> input = List.of("$");
        List<?> expected = Arrays.asList(null, null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testBlankInput() {
        List<String> requestedFields = List.of(DOMAIN, REGISTERED_DOMAIN);
        List<String> input = List.of("  ");
        List<?> expected = Arrays.asList(null, null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMultiLevelSubdomain() {
        List<String> requestedFields = List.of(DOMAIN, REGISTERED_DOMAIN, SUBDOMAIN);
        List<String> input = List.of("a.b.example.com");
        List<?> expected = List.of("a.b.example.com", "example.com", "a.b");
        evaluateAndCompare(input, requestedFields, expected);
    }

    /*****************************************************************************************************
     * Implementing AbstractCompoundOutputEvaluatorTests methods for the OperatorTestCase framework
     *****************************************************************************************************/

    @Override
    protected List<String> getRequestedFieldsForSimple() {
        return List.of(DOMAIN, REGISTERED_DOMAIN, eTLD, SUBDOMAIN);
    }

    @Override
    protected List<String> getSampleInputForSimple() {
        return List.of("www.example.co.uk", "elastic.co", "content-autofill.googleapis.com");
    }

    @Override
    protected List<Object[]> getExpectedOutputForSimple() {
        return List.of(
            new Object[] { "www.example.co.uk", "elastic.co", "content-autofill.googleapis.com" },
            new Object[] { "example.co.uk", "elastic.co", "googleapis.com" },
            new Object[] { "co.uk", "co", "com" },
            new Object[] { "www", null, "content-autofill" }
        );
    }
}
