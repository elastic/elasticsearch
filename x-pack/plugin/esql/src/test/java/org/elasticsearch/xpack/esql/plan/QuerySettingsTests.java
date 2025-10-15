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

import java.util.List;

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

    private static <T> void assertValid(QuerySettings.QuerySettingDef<T> settingDef, Literal valueLiteral, Matcher<T> parsedValueMatcher) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingDef.name(), valueLiteral));
        QuerySettings.validate(new EsqlStatement(null, List.of(setting)), null);

        QuerySettings settings = QuerySettings.from(new EsqlStatement(null, List.of(setting)));

        T value = settings.get(settingDef);

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

    private static <T> void assertDefault(QuerySettings.QuerySettingDef<T> settingDef, Matcher<? super T> defaultMatcher) {
        QuerySettings settings = QuerySettings.from(new EsqlStatement(null, List.of()));

        T value = settings.get(settingDef);

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
