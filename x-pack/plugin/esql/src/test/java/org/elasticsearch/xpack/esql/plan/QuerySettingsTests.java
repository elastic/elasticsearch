/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.hamcrest.Matcher;
import org.junit.AfterClass;

import java.time.ZoneId;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class QuerySettingsTests extends ESTestCase {
    public void testNonExistingSetting() {
        String settingName = "non_existing";

        assertInvalid(settingName, Literal.keyword(Source.EMPTY, "12"), "Unknown setting [" + settingName + "]");
    }

    public void testProjectRouting() {
        var setting = QuerySettings.PROJECT_ROUTING;

        assertValid(setting, Literal.keyword(Source.EMPTY, "my-project"), equalTo("my-project"));

        assertInvalid(
            setting.name(),
            new Literal(Source.EMPTY, 12, DataType.INTEGER),
            "Setting [" + setting.name() + "] must be of type KEYWORD"
        );
    }

    public void testTimeZone() {
        var setting = QuerySettings.TIME_ZONE;

        assertValid(setting, Literal.keyword(Source.EMPTY, "Europe/Madrid"), equalTo(ZoneId.of("Europe/Madrid")));
        assertValid(setting, Literal.keyword(Source.EMPTY, "+05:00"), equalTo(ZoneId.of("+05:00")));
        assertValid(setting, Literal.keyword(Source.EMPTY, "+05"), equalTo(ZoneId.of("+05")));
        assertValid(setting, Literal.keyword(Source.EMPTY, "+07:15"), equalTo(ZoneId.of("+07:15")));

        assertInvalid(setting.name(), Literal.integer(Source.EMPTY, 12), "Setting [" + setting.name() + "] must be of type KEYWORD");
        assertInvalid(
            setting.name(),
            Literal.keyword(Source.EMPTY, "Europe/New York"),
            "Error validating setting [" + setting.name() + "]: Invalid time zone [Europe/New York]"
        );
    }

    private static <T> void assertValid(
        QuerySettings.QuerySettingDef<T> settingDef,
        Expression valueExpression,
        Matcher<T> parsedValueMatcher
    ) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingDef.name(), valueExpression));
        QuerySettings.validate(new EsqlStatement(null, List.of(setting)), null);

        T value = settingDef.get(valueExpression, null);

        assertThat(value, parsedValueMatcher);
    }

    private static void assertInvalid(String settingName, Expression valueExpression, String expectedMessage) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingName, valueExpression));
        assertThat(
            expectThrows(ParsingException.class, () -> QuerySettings.validate(new EsqlStatement(null, List.of(setting)), null))
                .getMessage(),
            containsString(expectedMessage)
        );
    }

    @AfterClass
    public static void generateDocs() throws Exception {
        for (QuerySettings.QuerySettingDef<?> def : QuerySettings.ALL_SETTINGS) {
            DocsV3Support.SettingsDocsSupport settingsDocsSupport = new DocsV3Support.SettingsDocsSupport(
                def,
                QuerySettingsTests.class,
                DocsV3Support.callbacksFromSystemProperty()
            );
            settingsDocsSupport.renderDocs();
        }
    }
}
