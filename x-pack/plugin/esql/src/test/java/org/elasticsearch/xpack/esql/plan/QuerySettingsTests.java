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
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class QuerySettingsTests extends ESTestCase {
    public void testValidate_NonExistingSetting() {
        String settingName = "non_existing";

        assertInvalid(settingName, Literal.keyword(Source.EMPTY, "12"), "Unknown setting [" + settingName + "]");
    }

    public void testValidate_ProjectRouting() {
        var setting = QuerySettings.PROJECT_ROUTING;

        assertDefault(setting, nullValue());
        assertValid(setting, Literal.keyword(Source.EMPTY, "my-project"), equalTo("my-project"));

        assertInvalid(
            setting.name(),
            new Literal(Source.EMPTY, 12, DataType.INTEGER),
            "Setting [" + setting.name() + "] must be of type KEYWORD"
        );
    }

    public void testValidate_TimeZone() {
        var setting = QuerySettings.TIME_ZONE;

        assertDefault(setting, both(equalTo(ZoneId.of("Z"))).and(equalTo(ZoneOffset.UTC)));

        assertValid(setting, Literal.keyword(Source.EMPTY, "UTC"), equalTo(ZoneId.of("UTC")));
        assertValid(setting, Literal.keyword(Source.EMPTY, "Z"), both(equalTo(ZoneId.of("Z"))).and(equalTo(ZoneOffset.UTC)));
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

    private static <T> void assertValid(QuerySettings.QuerySettingDef<T> settingDef, Literal valueLiteral, Matcher<T> parsedValueMatcher) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingDef.name(), valueLiteral));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        QuerySettings.validate(statement, null);

        T value = statement.setting(settingDef);

        assertThat(value, parsedValueMatcher);
    }

    private static void assertInvalid(String settingName, Expression valueExpression, String expectedMessage) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingName, valueExpression));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        assertThat(
            expectThrows(ParsingException.class, () -> QuerySettings.validate(statement, null)).getMessage(),
            containsString(expectedMessage)
        );
    }

    private static <T> void assertDefault(QuerySettings.QuerySettingDef<T> settingDef, Matcher<? super T> defaultMatcher) {
        EsqlStatement statement = new EsqlStatement(null, List.of());

        T value = statement.setting(settingDef);

        assertThat(value, defaultMatcher);
    }

    @AfterClass
    public static void generateDocs() throws Exception {
        for (QuerySettings.QuerySettingDef<?> def : QuerySettings.SETTINGS_BY_NAME.values()) {
            DocsV3Support.SettingsDocsSupport settingsDocsSupport = new DocsV3Support.SettingsDocsSupport(
                def,
                QuerySettingsTests.class,
                DocsV3Support.callbacksFromSystemProperty()
            );
            settingsDocsSupport.renderDocs();
        }
    }
}
