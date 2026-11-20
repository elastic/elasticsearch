/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * The reserved value of the {@code param_any_value} setting is the only setting the parser needs while it builds the statement:
 * it starts from the request's value or the default, a {@code SET} in the statement overrides it, and release builds don't
 * recognize any. The tests run in snapshot and release builds, and require the capability for the build they check.
 */
public class ParamAnyValueParserTests extends ESTestCase {

    /** The parser of the running build, once the test requires the Any controls to be enabled in it. */
    private static EsqlParser parserWithAny() {
        assumeTrue("requires the Any controls", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
        return TEST_PARSER;
    }

    /** The parser of the running build, once the test requires the Any controls to be disabled in it. */
    private static EsqlParser parserWithoutAny() {
        assumeFalse("requires the Any controls to be disabled", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
        return TEST_PARSER;
    }

    /**
     * The value the statement builder ends with after visiting {@code query}, starting from {@code contextValue}. It runs the
     * builder on its own because the value is internal to the parsing: nothing reads it yet.
     */
    private static String valueAfterParsing(String query, @Nullable String contextValue) {
        EsqlConfig config = new EsqlConfig(TEST_FUNCTION_REGISTRY);
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));
        lexer.setEsqlConfig(config);
        EsqlBaseParser parser = new EsqlBaseParser(new CommonTokenStream(lexer));
        parser.setEsqlConfig(config);
        AstBuilder builder = new AstBuilder(
            new ExpressionBuilder.ParsingContext(
                new QueryParams(),
                new InferenceSettings(Settings.EMPTY),
                null,
                QueryPragmas.EMPTY,
                EsqlFlags.DEFAULTS,
                contextValue
            )
        );
        builder.statement(parser.statements());
        return builder.anyParamValue();
    }

    private static void parse(EsqlParser parser, String query, QueryParams params, Map<QuerySettingDef<?>, Object> requestSettings) {
        parser.parse(query, params, new InferenceSettings(Settings.EMPTY), QueryPragmas.EMPTY, EsqlFlags.DEFAULTS, requestSettings);
    }

    public void testStartsFromTheDefault() {
        assertThat(parserWithAny().anyParamValue(Map.of()), equalTo("__ANY__"));
    }

    public void testStartsFromTheRequestValue() {
        assertThat(parserWithAny().anyParamValue(Map.of(QuerySettings.PARAM_ANY_VALUE, "custom")), equalTo("custom"));
    }

    public void testEmptyRequestValueIsRejectedBeforeParsing() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> parse(parserWithAny(), "ROW x = 1", new QueryParams(), Map.of(QuerySettings.PARAM_ANY_VALUE, ""))
        );
        assertThat(e.getMessage(), containsString("Error validating setting [param_any_value]: must not be empty"));
    }

    public void testReleaseBuildsHaveNoReservedValue() {
        assertThat(parserWithoutAny().anyParamValue(Map.of()), is(nullValue()));
        // a request value is left to the settings resolution after the parsing, which rejects it on release builds
        assertThat(parserWithoutAny().anyParamValue(Map.of(QuerySettings.PARAM_ANY_VALUE, "custom")), is(nullValue()));
        parse(parserWithoutAny(), "ROW x = 1", new QueryParams(), Map.of(QuerySettings.PARAM_ANY_VALUE, "custom"));
    }

    public void testContextValueIsKeptWithoutSet() {
        assertThat(valueAfterParsing("ROW x = 1", "__ANY__"), equalTo("__ANY__"));
        assertThat(valueAfterParsing("SET unmapped_fields = \"nullify\"; ROW x = 1", "custom"), equalTo("custom"));
    }

    public void testNoReservedValueIsKept() {
        assertThat(valueAfterParsing("ROW x = 1", null), is(nullValue()));
        // a SET can't turn the feature on: on release builds it's rejected, and the context has no value
        assertThat(valueAfterParsing("SET param_any_value = \"custom\"; ROW x = 1", null), is(nullValue()));
    }

    public void testSetOverridesContextValue() {
        assertThat(valueAfterParsing("SET param_any_value = \"custom\"; ROW x = 1", "__ANY__"), equalTo("custom"));
    }

    public void testLastSetWins() {
        assertThat(
            valueAfterParsing("SET param_any_value = \"first\"; SET param_any_value = \"last\"; ROW x = 1", "__ANY__"),
            equalTo("last")
        );
    }

    public void testSetWithAnotherTypeKeepsTheValue() {
        // the settings validation rejects these right after the parsing
        assertThat(valueAfterParsing("SET param_any_value = 1; ROW x = 1", "__ANY__"), equalTo("__ANY__"));
        assertThat(valueAfterParsing("SET param_any_value = true; ROW x = 1", "__ANY__"), equalTo("__ANY__"));
        assertThat(valueAfterParsing("SET param_any_value = [\"a\", \"b\"]; ROW x = 1", "__ANY__"), equalTo("__ANY__"));
        assertThat(valueAfterParsing("SET param_any_value = {\"a\": 1}; ROW x = 1", "__ANY__"), equalTo("__ANY__"));
    }

    public void testEmptySetKeepsTheValue() {
        assertThat(valueAfterParsing("SET param_any_value = \"\"; ROW x = 1", "custom"), equalTo("custom"));
    }

    public void testSetWithAParameterFails() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("v", "*")));
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse(parserWithAny(), "SET param_any_value = ?v; ROW x = 1", params, Map.of())
        );
        assertThat(e.getMessage(), containsString("Setting [param_any_value] can't be set with a parameter"));
    }

    public void testSetWithAParameterIsLeftToTheValidationInReleaseBuilds() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("v", "*")));
        // the parser doesn't act on the setting in release builds, where the validation rejects it
        parse(parserWithoutAny(), "SET param_any_value = ?v; ROW x = 1", params, Map.of());
    }

    public void testSetIsRejectedInReleaseBuilds() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parserWithoutAny().createStatement("SET param_any_value = \"custom\"; ROW x = 1")
        );
        assertThat(e.getMessage(), containsString("Setting [param_any_value] is only available in snapshot builds"));
    }

    public void testSetWithAnotherTypeIsRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parserWithAny().createStatement("SET param_any_value = 1; ROW x = 1")
        );
        assertThat(e.getMessage(), containsString("Setting [param_any_value] must be of type KEYWORD"));
    }

    public void testEmptySetIsRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parserWithAny().createStatement("SET param_any_value = \"\"; ROW x = 1")
        );
        assertThat(e.getMessage(), containsString("Error validating setting [param_any_value]: must not be empty"));
    }
}
