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
import org.junit.BeforeClass;

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
 * it starts from the request's value or the default, and a {@code SET} in the statement overrides it. The feature is snapshot-only,
 * so the tests require its capability.
 */
public class ParamAnyValueParserTests extends ESTestCase {

    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any controls", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
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
        assertThat(TEST_PARSER.anyParamValue(Map.of()), equalTo("__ANY__"));
    }

    public void testStartsFromTheRequestValue() {
        assertThat(TEST_PARSER.anyParamValue(Map.of(QuerySettings.PARAM_ANY_VALUE, "custom")), equalTo("custom"));
    }

    public void testEmptyRequestValueIsRejectedBeforeParsing() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> parse(TEST_PARSER, "ROW x = 1", new QueryParams(), Map.of(QuerySettings.PARAM_ANY_VALUE, ""))
        );
        assertThat(e.getMessage(), containsString("Error validating setting [param_any_value]: must not be empty"));
    }

    public void testContextValueIsKeptWithoutSet() {
        assertThat(valueAfterParsing("ROW x = 1", "__ANY__"), equalTo("__ANY__"));
        assertThat(valueAfterParsing("SET unmapped_fields = \"nullify\"; ROW x = 1", "custom"), equalTo("custom"));
    }

    public void testNoReservedValueIsKept() {
        assertThat(valueAfterParsing("ROW x = 1", null), is(nullValue()));
        // a SET can't turn the feature on when the context has no value
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
            () -> parse(TEST_PARSER, "SET param_any_value = ?v; ROW x = 1", params, Map.of())
        );
        assertThat(e.getMessage(), containsString("Setting [param_any_value] can't be set with a parameter"));
    }

    public void testSetWithAnotherTypeIsRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> TEST_PARSER.createStatement("SET param_any_value = 1; ROW x = 1")
        );
        assertThat(e.getMessage(), containsString("Setting [param_any_value] must be of type KEYWORD"));
    }

    public void testEmptySetIsRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> TEST_PARSER.createStatement("SET param_any_value = \"\"; ROW x = 1")
        );
        assertThat(e.getMessage(), containsString("Error validating setting [param_any_value]: must not be empty"));
    }
}
