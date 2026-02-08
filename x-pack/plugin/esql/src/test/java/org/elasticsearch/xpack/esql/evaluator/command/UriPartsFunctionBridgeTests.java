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

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.DOMAIN;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.EXTENSION;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.FRAGMENT;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PASSWORD;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PATH;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PORT;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.QUERY;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.SCHEME;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.USERNAME;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.USER_INFO;

public class UriPartsFunctionBridgeTests extends AbstractCompoundOutputEvaluatorTests {

    @Override
    protected CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector(List<String> requestedFields) {
        return new UriPartsFunctionBridge.UriPartsCollectorImpl(requestedFields);
    }

    @Override
    protected Map<String, Class<?>> getSupportedOutputFieldMappings() {
        return UriPartsFunctionBridge.getAllOutputFields();
    }

    public void testFullOutput() {
        List<String> requestedFields = List.of(SCHEME, DOMAIN, PORT, PATH, EXTENSION, QUERY, FRAGMENT, USER_INFO, USERNAME, PASSWORD);
        List<String> input = List.of("http://user:pass@example.com:8080/path/to/file.html?query=val#fragment");
        List<Object[]> expected = List.of(
            new Object[] { "http" },
            new Object[] { "example.com" },
            new Object[] { 8080 },
            new Object[] { "/path/to/file.html" },
            new Object[] { "html" },
            new Object[] { "query=val" },
            new Object[] { "fragment" },
            new Object[] { "user:pass" },
            new Object[] { "user" },
            new Object[] { "pass" }
        );
        evaluateAndCompare(input, requestedFields, expected);
    }

    /*public void testMultiValue() {
        List<String> requestedFields = List.of(SCHEME, DOMAIN, PORT, PATH, EXTENSION, QUERY, FRAGMENT, USER_INFO, USERNAME, PASSWORD);
        List<String> input = List.of(
            "http://user:pass@example.com:8080/path/to/file.html?query=val#fragment",
            "https://elastic.co/downloads",
            "ftp://ftp.example.org/resource.txt"
        );
        List<Object[]> expected = List.of(
            new Object[] { "http", "https", "ftp" },
            new Object[] { "example.com", "elastic.co", "ftp.example.org" },
            new Object[] { 8080, null, null },
            new Object[] { "/path/to/file.html", "/downloads", "/resource.txt" },
            new Object[] { "html", null, "txt" },
            new Object[] { "query=val", null, null },
            new Object[] { "fragment", null, null },
            new Object[] { "user:pass", null, null },
            new Object[] { "user", null, null },
            new Object[] { "pass", null, null }
        );
        evaluateAndCompare(input, requestedFields, expected);
    }*/

    public void testPartialFieldsRequested() {
        List<String> requestedFields = List.of(DOMAIN, PORT);
        List<String> input = List.of("http://user:pass@example.com:8080/path/to/file.html?query=val#fragment");
        List<Object[]> expected = List.of(new Object[] { "example.com" }, new Object[] { 8080 });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingPortAndUserInfo() {
        List<String> requestedFields = List.of(SCHEME, DOMAIN, PORT, USERNAME);
        List<String> input = List.of("https://elastic.co/downloads");
        List<Object[]> expected = List.of(
            new Object[] { "https" },
            new Object[] { "elastic.co" },
            new Object[] { null },
            new Object[] { null }
        );
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingExtension() {
        List<String> requestedFields = List.of(PATH, EXTENSION);
        List<String> input = List.of("https://elastic.co/downloads");
        List<Object[]> expected = List.of(new Object[] { "/downloads" }, new Object[] { null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testAllMissingFields() {
        List<String> requestedFields = List.of(FRAGMENT, QUERY, USER_INFO);
        List<String> input = List.of("https://elastic.co/downloads");
        List<Object[]> expected = List.of(new Object[] { null }, new Object[] { null }, new Object[] { null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testInvalidInput() {
        List<String> requestedFields = List.of(DOMAIN, PORT);
        List<String> input = List.of("not a valid url");
        List<Object[]> expected = List.of(new Object[] { null }, new Object[] { null });
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new WarningSourceLocation() {
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
        evaluateAndCompare(input, requestedFields, expected, warnings);
        assertCriticalWarnings(
            "Line 1:2: evaluation of [invalid_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.lang.IllegalArgumentException: unable to parse URI [not a valid url]"
        );
    }

    /*****************************************************************************************************
     * Implementing AbstractCompoundOutputEvaluatorTests methods for the OperatorTestCase framework
     *****************************************************************************************************/

    @Override
    protected List<String> getRequestedFieldsForSimple() {
        return List.of(SCHEME, DOMAIN, PORT, PATH, EXTENSION, QUERY, FRAGMENT, USER_INFO, USERNAME, PASSWORD);
    }

    @Override
    protected List<String> getSampleInputForSimple() {
        return List.of(
            "http://user:pass@example.com:8080/path/to/file.html?query=val#fragment",
            "https://elastic.co/downloads",
            "ftp://ftp.example.org/resource.txt"
        );
    }

    @Override
    protected List<Object[]> getExpectedOutputForSimple() {
        return List.of(
            new Object[] { "http", "https", "ftp" },
            new Object[] { "example.com", "elastic.co", "ftp.example.org" },
            new Object[] { 8080, null, null },
            new Object[] { "/path/to/file.html", "/downloads", "/resource.txt" },
            new Object[] { "html", null, "txt" },
            new Object[] { "query=val", null, null },
            new Object[] { "fragment", null, null },
            new Object[] { "user:pass", null, null },
            new Object[] { "user", null, null },
            new Object[] { "pass", null, null }
        );
    }
}
