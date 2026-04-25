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
import org.elasticsearch.useragent.api.Details;
import org.elasticsearch.useragent.api.UserAgentParser;
import org.elasticsearch.useragent.api.VersionedName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.useragent.api.UserAgentParsedInfo.DEVICE_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.DEVICE_TYPE;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_FULL;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_VERSION;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.VERSION;

public class UserAgentFunctionBridgeTests extends AbstractCompoundOutputEvaluatorTests {

    private static final String CHROME_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 "
        + "(KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36";
    private static final String FIREFOX_UA = "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0";
    private static final String CURL_UA = "curl/7.68.0";

    /**
     * A test parser that returns known Details for known user-agent strings.
     */
    private static final UserAgentParser TEST_PARSER = (agentString, extractDeviceType) -> {
        if (CHROME_UA.equals(agentString)) {
            return new Details(
                "Chrome",
                "33.0.1750",
                new VersionedName("Mac OS X", "10.9.2"),
                "Mac OS X 10.9.2",
                new VersionedName("Mac", null),
                extractDeviceType ? "Desktop" : null
            );
        } else if (FIREFOX_UA.equals(agentString)) {
            return new Details(
                "Firefox",
                "115.0",
                new VersionedName("Linux", null),
                "Linux",
                new VersionedName("Other", null),
                extractDeviceType ? "Desktop" : null
            );
        } else if (CURL_UA.equals(agentString)) {
            return new Details("curl", "7.68.0", null, null, null, null);
        }
        return new Details(null, null, null, null, null, null);
    };

    private boolean extractDeviceType = false;

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
        return new UserAgentFunctionBridge.UserAgentCollectorImpl(requestedFields, TEST_PARSER, extractDeviceType);
    }

    @Override
    protected String collectorSimpleName() {
        return UserAgentFunctionBridge.UserAgentCollectorImpl.class.getSimpleName();
    }

    @Override
    protected Map<String, Class<?>> getSupportedOutputFieldMappings() {
        return UserAgentFunctionBridge.getAllOutputFields();
    }

    public void testFullOutput() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, VERSION, OS_NAME, OS_VERSION, OS_FULL, DEVICE_NAME);
        List<String> input = List.of(CHROME_UA);
        List<?> expected = List.of("Chrome", "33.0.1750", "Mac OS X", "10.9.2", "Mac OS X 10.9.2", "Mac");
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testFullOutputWithDeviceType() {
        extractDeviceType = true;
        List<String> requestedFields = List.of(NAME, VERSION, OS_NAME, OS_VERSION, OS_FULL, DEVICE_NAME, DEVICE_TYPE);
        List<String> input = List.of(CHROME_UA);
        List<?> expected = List.of("Chrome", "33.0.1750", "Mac OS X", "10.9.2", "Mac OS X 10.9.2", "Mac", "Desktop");
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testDeviceTypeNotExtracted() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, DEVICE_TYPE);
        List<String> input = List.of(CHROME_UA);
        List<?> expected = Arrays.asList("Chrome", null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testPartialFieldsRequested() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, VERSION);
        List<String> input = List.of(CHROME_UA);
        List<?> expected = List.of("Chrome", "33.0.1750");
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testNoOsMatch() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, OS_NAME, OS_VERSION, OS_FULL);
        List<String> input = List.of(CURL_UA);
        List<?> expected = Arrays.asList("curl", null, null, null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testNoDeviceMatch() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, DEVICE_NAME);
        List<String> input = List.of(CURL_UA);
        List<?> expected = Arrays.asList("curl", null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testUnknownInput() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, VERSION, OS_NAME);
        List<String> input = List.of("completely unknown agent string");
        List<?> expected = Arrays.asList(null, null, null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testUnknownInputWithDeviceType() {
        extractDeviceType = true;
        List<String> requestedFields = List.of(NAME, VERSION, OS_NAME, OS_VERSION, OS_FULL, DEVICE_NAME, DEVICE_TYPE);
        List<String> input = List.of("completely unknown agent string");
        List<?> expected = Arrays.asList(null, null, null, null, null, null, null);
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMultiValue() {
        extractDeviceType = false;
        List<String> requestedFields = List.of(NAME, VERSION, OS_NAME, OS_VERSION, OS_FULL, DEVICE_NAME);
        List<String> input = List.of(CHROME_UA, FIREFOX_UA);
        List<Object[]> expected = Collections.nCopies(requestedFields.size(), new Object[] { null });
        evaluateAndCompare(input, requestedFields, expected, WARNINGS);
        assertCriticalWarnings(
            "Line 1:2: evaluation of [invalid_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.lang.IllegalArgumentException: This command doesn't support multi-value input"
        );
    }

    /*****************************************************************************************************
     * Implementing AbstractCompoundOutputEvaluatorTests methods for the OperatorTestCase framework
     *****************************************************************************************************/

    @Override
    protected List<String> getRequestedFieldsForSimple() {
        return List.of(NAME, VERSION, OS_NAME, OS_VERSION, OS_FULL, DEVICE_NAME);
    }

    @Override
    protected List<String> getSampleInputForSimple() {
        return List.of(CHROME_UA, FIREFOX_UA, CURL_UA);
    }

    @Override
    protected List<Object[]> getExpectedOutputForSimple() {
        return List.of(
            new Object[] { "Chrome", "Firefox", "curl" },
            new Object[] { "33.0.1750", "115.0", "7.68.0" },
            new Object[] { "Mac OS X", "Linux", null },
            new Object[] { "10.9.2", null, null },
            new Object[] { "Mac OS X 10.9.2", "Linux", null },
            new Object[] { "Mac", "Other", null }
        );
    }
}
