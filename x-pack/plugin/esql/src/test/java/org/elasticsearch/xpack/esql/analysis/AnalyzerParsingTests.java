/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.StatementParserTests;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Parses a plan, builds an AST for it, and then runs logical analysis on it.
 * So if we don't error out in the process,  all references were resolved correctly.
 * Use this class if you want to test parsing <b>and resolution</b> of a query
 *  and especially if you expect to get a ParsingException.
 *  <p>
 *  For testing parsing <b>only</b>, use {@link StatementParserTests} or a subclass of {@link AbstractStatementParserTests}.
 */
public class AnalyzerParsingTests extends ESTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> params() {
        // Run every test at both the current transport version and the pre-existing per-build random one, so a
        // version-gated analyzer change surfaces here in its own PR rather than only under a lucky random draw.
        return List.of(new Object[] { "current", true }, new Object[] { "historical", false });
    }

    private final boolean pinCurrentVersion;

    public AnalyzerParsingTests(@SuppressWarnings("unused") String name, boolean pinCurrentVersion) {
        this.pinCurrentVersion = pinCurrentVersion;
        this.defaultAnalyzer = analyzer().addEmployees("test");
    }

    /**
     * Shadows {@link EsqlTestUtils#analyzer()} so every analyzer this suite builds honors the run's transport version:
     * {@code current} pins {@link TransportVersion#current()}; {@code historical} leaves {@link TestAnalyzer}'s default
     * random compatible version untouched, so that mode is behavior-identical to before the suite was parametrized.
     */
    private TestAnalyzer analyzer() {
        TestAnalyzer analyzer = EsqlTestUtils.analyzer();
        return pinCurrentVersion ? analyzer.minimumTransportVersion(TransportVersion.current()) : analyzer;
    }

    private final TestAnalyzer defaultAnalyzer;

    public void testCaseFunctionInvalidInputs() {
        defaultAnalyzer.error(
            "row a = 1 | eval x = case()",
            ParsingException.class,
            equalTo("line 1:22: error building [case]: expects at least two arguments")
        );
        defaultAnalyzer.error(
            "row a = 1 | eval x = case(a)",
            ParsingException.class,
            equalTo("line 1:22: error building [case]: expects at least two arguments")
        );
        defaultAnalyzer.error(
            "row a = 1 | eval x = case(1)",
            ParsingException.class,
            equalTo("line 1:22: error building [case]: expects at least two arguments")
        );
    }

    public void testConcatFunctionInvalidInputs() {
        defaultAnalyzer.error(
            "row a = 1 | eval x = concat()",
            ParsingException.class,
            equalTo("line 1:22: error building [concat]: expects at least two arguments")
        );
        defaultAnalyzer.error(
            "row a = 1 | eval x = concat(a)",
            ParsingException.class,
            equalTo("line 1:22: error building [concat]: expects at least two arguments")
        );
        defaultAnalyzer.error(
            "row a = 1 | eval x = concat(1)",
            ParsingException.class,
            equalTo("line 1:22: error building [concat]: expects at least two arguments")
        );
    }

    public void testCoalesceFunctionInvalidInputs() {
        defaultAnalyzer.error(
            "row a = 1 | eval x = coalesce()",
            ParsingException.class,
            equalTo("line 1:22: error building [coalesce]: expects at least one argument")
        );
    }

    public void testGreatestFunctionInvalidInputs() {
        defaultAnalyzer.error(
            "row a = 1 | eval x = greatest()",
            ParsingException.class,
            equalTo("line 1:22: error building [greatest]: expects at least one argument")
        );
    }

    public void testLeastFunctionInvalidInputs() {
        defaultAnalyzer.error(
            "row a = 1 | eval x = least()",
            ParsingException.class,
            equalTo("line 1:22: error building [least]: expects at least one argument")
        );
    }
}
