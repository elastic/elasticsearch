/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.hamcrest.Matcher;
import org.junit.AfterClass;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomizeCase;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QuerySettingsTests extends ESTestCase {

    private static SettingsValidationContext NON_SNAPSHOT_CTX_WITH_CPS_ENABLED = new SettingsValidationContext(true, false);

    private static SettingsValidationContext SNAPSHOT_CTX_WITH_CPS_ENABLED = new SettingsValidationContext(true, true);

    private static SettingsValidationContext SNAPSHOT_CTX_WITH_CPS_DISABLED = new SettingsValidationContext(false, true);

    public void testValidate_NonExistingSetting() {
        String settingName = "non_existing";

        assertInvalid(settingName, of("12"), "Unknown setting [" + settingName + "]");
    }

    public void testValidate_ProjectRouting() {
        var setting = QuerySettings.PROJECT_ROUTING;

        assertDefault(setting, nullValue());
        assertValid(setting, of("my-project"), equalTo("my-project"));

        assertInvalid(
            setting.name(),
            new Literal(Source.EMPTY, 12, DataType.INTEGER),
            "Setting [" + setting.name() + "] must be of type KEYWORD"
        );
    }

    public void testValidate_ProjectRouting_noCps() {
        var setting = QuerySettings.PROJECT_ROUTING;
        assertValid(setting, Literal.keyword(Source.EMPTY, "my-project"), equalTo("my-project"), NON_SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertInvalid(
            setting.name(),
            SNAPSHOT_CTX_WITH_CPS_DISABLED,
            of("my-project"),
            "Error validating setting [project_routing]: cross-project search not enabled"
        );
    }

    public void testValidate_TimeZone() {
        var setting = QuerySettings.TIME_ZONE;

        assertDefault(setting, both(equalTo(ZoneId.of("Z"))).and(equalTo(ZoneOffset.UTC)));

        assertValid(setting, of("UTC"), equalTo(ZoneId.of("UTC")));
        assertValid(setting, of("Z"), both(equalTo(ZoneId.of("Z"))).and(equalTo(ZoneOffset.UTC)));
        assertValid(setting, of("Europe/Madrid"), equalTo(ZoneId.of("Europe/Madrid")));
        assertValid(setting, of("+05:00"), equalTo(ZoneId.of("+05:00")));
        assertValid(setting, of("+05"), equalTo(ZoneId.of("+05")));
        assertValid(setting, of("+07:15"), equalTo(ZoneId.of("+07:15")));

        assertInvalid(setting.name(), Literal.integer(Source.EMPTY, 12), "Setting [" + setting.name() + "] must be of type KEYWORD");
        assertInvalid(
            setting.name(),
            of("Europe/New York"),
            "Error validating setting [" + setting.name() + "]: Invalid time zone [Europe/New York]"
        );
    }

    public void testValidate_TimeZone_techPreview() {
        var setting = QuerySettings.TIME_ZONE;
        assertValid(setting, of("UTC"), equalTo(ZoneId.of("UTC")), NON_SNAPSHOT_CTX_WITH_CPS_ENABLED);
    }

    public void testValidate_UnmappedFields_techPreview() {
        assumeFalse("Requires no snapshot", Build.current().isSnapshot());

        validateUnmappedFields("FAIL", "NULLIFY");
        var settingName = QuerySettings.UNMAPPED_FIELDS.name();
        assertInvalid(
            settingName,
            NON_SNAPSHOT_CTX_WITH_CPS_ENABLED,
            of("UNKNOWN"),
            "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of [FAIL, NULLIFY]"
        );
    }

    public void testValidate_UnmappedFields_allValues() {
        assumeTrue("Requires unmapped fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled());
        validateUnmappedFields("FAIL", "NULLIFY", "LOAD");
    }

    private void validateUnmappedFields(String... values) {
        var setting = QuerySettings.UNMAPPED_FIELDS;

        assertDefault(setting, equalTo(UnmappedResolution.FAIL));

        for (String value : values) {
            assertValid(setting, of(randomizeCase(value)), equalTo(UnmappedResolution.valueOf(value)));
        }

        assertInvalid(setting.name(), of(12), "Setting [" + setting.name() + "] must be of type KEYWORD");
        assertInvalid(
            setting.name(),
            of("UNKNOWN"),
            "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of "
                + Arrays.toString(values)
        );
    }

    public void testValidate_Approximation() {
        var def = QuerySettings.APPROXIMATION;
        assertDefault(def, is(nullValue()));
        {
            QuerySetting setting = new QuerySetting(
                Source.EMPTY,
                new Alias(Source.EMPTY, def.name(), Literal.fromBoolean(Source.EMPTY, true))
            );
            EsqlStatement statement = new EsqlStatement(null, List.of(setting));
            QuerySettings.validate(statement, SNAPSHOT_CTX_WITH_CPS_DISABLED);
            assertThat(statement.setting(def), is(ApproximationSettings.DEFAULT));
        }
        {
            QuerySetting setting = new QuerySetting(
                Source.EMPTY,
                new Alias(Source.EMPTY, def.name(), Literal.fromBoolean(Source.EMPTY, false))
            );
            EsqlStatement statement = new EsqlStatement(null, List.of(setting));
            QuerySettings.validate(statement, SNAPSHOT_CTX_WITH_CPS_DISABLED);
            assertThat(statement.setting(def), is(nullValue()));
        }

        assertValid(def, new MapExpression(Source.EMPTY, List.of()), equalTo(ApproximationSettings.DEFAULT));
        assertValid(
            def,
            new MapExpression(
                Source.EMPTY,
                List.of(
                    Literal.keyword(Source.EMPTY, "rows"),
                    Literal.integer(Source.EMPTY, 10000),
                    Literal.keyword(Source.EMPTY, "confidence_level"),
                    Literal.fromDouble(Source.EMPTY, 0.9)
                )
            ),
            equalTo(new ApproximationSettings(10000, 0.9))
        );

        assertInvalid(
            def.name(),
            new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "rows"), Literal.integer(Source.EMPTY, 9999))),
            "line -1:-1: Error validating setting [approximation]: Approximation configuration [rows] must be at least 10000"
        );

        assertInvalid(
            def.name(),
            new MapExpression(
                Source.EMPTY,
                List.of(Literal.keyword(Source.EMPTY, "confidence_level"), Literal.fromDouble(Source.EMPTY, 0.999))
            ),
            "line -1:-1: Error validating setting [approximation]: "
                + "Approximation configuration [confidence_level] must be between 0.5 and 0.95"
        );

        assertInvalid(
            def.name(),
            Literal.integer(Source.EMPTY, 12),
            "line -1:-1: Error validating setting [approximation]: Invalid approximation configuration [12]"
        );

        assertInvalid(
            def.name(),
            Literal.keyword(Source.EMPTY, "foo"),
            "line -1:-1: Error validating setting [approximation]: Invalid approximation configuration [foo]"
        );

        assertInvalid(
            def.name(),
            new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "foo"), Literal.integer(Source.EMPTY, 10))),
            "line -1:-1: Error validating setting [approximation]: Approximation configuration contains unknown key [foo]"
        );
    }

    private static <T> void assertValid(QuerySettings.QuerySettingDef<T> settingDef, Expression value, Matcher<T> parsedValueMatcher) {
        assertValid(settingDef, value, parsedValueMatcher, SNAPSHOT_CTX_WITH_CPS_ENABLED);
    }

    private static <T> void assertValid(
        QuerySettings.QuerySettingDef<T> settingDef,
        Expression value,
        Matcher<T> parsedValueMatcher,
        SettingsValidationContext ctx
    ) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingDef.name(), value));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        QuerySettings.validate(statement, ctx);

        T val = statement.setting(settingDef);

        assertThat(val, parsedValueMatcher);
    }

    private static void assertInvalid(String settingName, Expression valueExpression, String expectedMessage) {
        assertInvalid(settingName, SNAPSHOT_CTX_WITH_CPS_ENABLED, valueExpression, expectedMessage);
    }

    private static void assertInvalid(
        String settingName,
        SettingsValidationContext ctx,
        Expression valueExpression,
        String expectedMessage
    ) {
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingName, valueExpression));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        assertThat(
            expectThrows(ParsingException.class, () -> QuerySettings.validate(statement, ctx)).getMessage(),
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
        List<QuerySettings.QuerySettingDef<?>> settings = QuerySettings.SETTINGS_BY_NAME.values()
            .stream()
            // TODO this is non-snapshot, but we don't want to expose it yet
            .filter(def -> def != QuerySettings.PROJECT_ROUTING)
            .sorted(Comparator.comparing(QuerySettings.QuerySettingDef::name))
            .toList();

        for (QuerySettings.QuerySettingDef<?> def : settings) {
            DocsV3Support.SettingsDocsSupport settingsDocsSupport = new DocsV3Support.SettingsDocsSupport(
                def,
                QuerySettingsTests.class,
                DocsV3Support.callbacksFromSystemProperty()
            );
            settingsDocsSupport.renderDocs();
        }

        // TODO remote this when project routing is public
        // we only want the Kibana JSON for now
        new DocsV3Support.SettingsDocsSupport(
            QuerySettings.PROJECT_ROUTING,
            QuerySettingsTests.class,
            DocsV3Support.callbacksFromSystemProperty()
        ).renderKibanaCommandDefinition();

        DocsV3Support.SettingsTocDocsSupport toc = new DocsV3Support.SettingsTocDocsSupport(
            settings,
            QuerySettingsTests.class,
            DocsV3Support.callbacksFromSystemProperty()
        );
        toc.renderDocs();
    }
}
