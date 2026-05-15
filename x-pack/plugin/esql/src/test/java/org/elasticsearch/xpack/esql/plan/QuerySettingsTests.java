/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.test.ESTestCase;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomizeCase;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QuerySettingsTests extends ESTestCase {

    private static SettingsValidationContext NON_SNAPSHOT_CTX_WITH_CPS_ENABLED = new SettingsValidationContext(true, false);

    private static SettingsValidationContext SNAPSHOT_CTX_WITH_CPS_ENABLED = new SettingsValidationContext(true, true);

    private static SettingsValidationContext SNAPSHOT_CTX_WITH_CPS_DISABLED = new SettingsValidationContext(false, true);

    private static List<SettingsValidationContext> allSettingsValidationContexts = List.of(
        NON_SNAPSHOT_CTX_WITH_CPS_ENABLED,
        SNAPSHOT_CTX_WITH_CPS_ENABLED,
        SNAPSHOT_CTX_WITH_CPS_DISABLED
    );

    public void testValidate_NonExistingSetting() {
        // Unknown SET keys in the query string warn and are skipped — they do not throw.
        // (The body surface — settings.{} — is the strict one; that path is covered by RequestXContent tests.)
        String settingName = "non_existing";
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingName, of("12")));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        QuerySettings.validate(statement, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertWarnings("Unknown ES|QL setting [" + settingName + "] — ignored");
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

    public void testValidate_UnmappedFields() {
        var setting = QuerySettings.UNMAPPED_FIELDS;
        String[] values = new String[] { "DEFAULT", "NULLIFY", "LOAD" };

        assertDefault(setting, equalTo(UnmappedResolution.DEFAULT));

        for (String value : values) {
            assertValid(setting, of(randomizeCase(value)), equalTo(UnmappedResolution.valueOf(value)));
        }

        assertInvalid(setting.name(), of(12), "Setting [" + setting.name() + "] must be of type KEYWORD");

        for (SettingsValidationContext ctx : allSettingsValidationContexts) {
            assertInvalid(
                setting.name(),
                ctx,
                of("UNKNOWN"),
                "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of "
                    + Arrays.toString(values)
            );
        }

        Source settingSource = new Source(3, 10, "SET unmapped_fields = \"UNKNOWN\"");
        assertInvalidWithSource(
            setting.name(),
            settingSource,
            of("UNKNOWN"),
            "line 3:11: Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of "
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
            assertThat(statement.setting(def), is(ApproximationSettings.EXPLICIT_NULL));
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

        Source settingSource = new Source(2, 5, "SET approximation = ...");
        assertInvalidWithSource(
            def.name(),
            settingSource,
            new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "rows"), Literal.integer(Source.EMPTY, 9999))),
            "line 2:6: Error validating setting [approximation]: Approximation configuration [rows] must be at least 10000"
        );

        assertInvalidWithSource(
            def.name(),
            settingSource,
            new MapExpression(
                Source.EMPTY,
                List.of(Literal.keyword(Source.EMPTY, "confidence_level"), Literal.fromDouble(Source.EMPTY, 0.999))
            ),
            "line 2:6: Error validating setting [approximation]: "
                + "Approximation configuration [confidence_level] must be between 0.5 and 0.95"
        );

        assertInvalidWithSource(
            def.name(),
            settingSource,
            Literal.integer(Source.EMPTY, 12),
            "line 2:6: Error validating setting [approximation]: Invalid approximation configuration [12]"
        );

        assertInvalidWithSource(
            def.name(),
            settingSource,
            Literal.keyword(Source.EMPTY, "foo"),
            "line 2:6: Error validating setting [approximation]: Invalid approximation configuration [foo]"
        );

        assertInvalidWithSource(
            def.name(),
            settingSource,
            new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "foo"), Literal.integer(Source.EMPTY, 10))),
            "line 2:6: Error validating setting [approximation]: Approximation configuration contains unknown key [foo]"
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
        assertInvalidWithSource(settingName, Source.EMPTY, ctx, valueExpression, expectedMessage);
    }

    private static void assertInvalidWithSource(
        String settingName,
        Source settingSource,
        Expression valueExpression,
        String expectedMessage
    ) {
        assertInvalidWithSource(settingName, settingSource, SNAPSHOT_CTX_WITH_CPS_ENABLED, valueExpression, expectedMessage);
    }

    private static void assertInvalidWithSource(
        String settingName,
        Source settingSource,
        SettingsValidationContext ctx,
        Expression valueExpression,
        String expectedMessage
    ) {
        QuerySetting setting = new QuerySetting(settingSource, new Alias(Source.EMPTY, settingName, valueExpression));
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

    public void testResolve_EmptySources() {
        EffectiveSettings effective = QuerySettings.resolve(Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(effective.consumedSettingNames(), is(java.util.Collections.emptySet()));
        // Default for time_zone is UTC
        assertThat(effective.get(QuerySettings.TIME_ZONE), equalTo(ZoneOffset.UTC));
        // Defaults for the rest are null
        assertThat(effective.get(QuerySettings.PROJECT_ROUTING), is(nullValue()));
        assertThat(effective.get(QuerySettings.APPROXIMATION), is(nullValue()));
    }

    public void testResolve_RequestParameterAppliesWhenNoQuerySet() {
        Map<QuerySettings.QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"));
        EffectiveSettings effective = QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(effective.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
        assertThat(effective.consumedSettingNames(), equalTo(java.util.Set.of("time_zone")));
    }

    public void testResolve_QuerySetOverridesRequestParameter() {
        // Request says Europe/Paris, query SET says UTC → query SET wins.
        Map<QuerySettings.QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"));
        QuerySetting set = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "time_zone", of("UTC")));
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        EffectiveSettings effective = QuerySettings.resolve(requestParams, statement, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(effective.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("UTC")));
        assertThat(effective.consumedSettingNames(), equalTo(java.util.Set.of("time_zone")));
    }

    public void testResolve_ApproximationFieldLevelMerge() {
        // Request supplies {rows: 10000}, query SET supplies {confidence_level: 0.92}. With field-level merge,
        // the resolved value carries both. (Last-wins would drop rows.)
        Map<QuerySettings.QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.APPROXIMATION, new ApproximationSettings(10000, 0.90));
        QuerySetting set = new QuerySetting(
            Source.EMPTY,
            new Alias(
                Source.EMPTY,
                "approximation",
                new MapExpression(
                    Source.EMPTY,
                    List.of(Literal.keyword(Source.EMPTY, "confidence_level"), Literal.fromDouble(Source.EMPTY, 0.92))
                )
            )
        );
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        EffectiveSettings effective = QuerySettings.resolve(requestParams, statement, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        ApproximationSettings resolved = effective.get(QuerySettings.APPROXIMATION);
        assertThat(resolved, is(new ApproximationSettings(10000, 0.92)));
    }

    public void testResolve_UnmappedFieldsIsSetOnly() {
        // UNMAPPED_FIELDS opted out of body exposure. The registry exposure flag is false.
        assertThat(QuerySettings.UNMAPPED_FIELDS.requestParameterExposed(), is(false));
        assertThat(QuerySettings.UNMAPPED_FIELDS.additionalBindings().isEmpty(), is(true));
    }

    public void testResolve_BodyExposedSettingsDeclareBindings() {
        // The three body-exposed settings each carry exactly one additional binding at the top level of the body,
        // mirroring the legacy field names.
        for (QuerySettings.QuerySettingDef<?> def : List.of(
            QuerySettings.TIME_ZONE,
            QuerySettings.PROJECT_ROUTING,
            QuerySettings.APPROXIMATION
        )) {
            assertThat("requestParameterExposed for [" + def.name() + "]", def.requestParameterExposed(), is(true));
            assertThat("jsonValueParser for [" + def.name() + "]", def.jsonValueParser(), is(notNullValue()));
            assertThat("additionalBindings for [" + def.name() + "]", def.additionalBindings(), hasSize(1));
            QuerySettings.RequestBodyBinding binding = def.additionalBindings().get(0);
            assertThat(binding.isAtRoot(), is(true));
            assertThat(binding.name(), equalTo(def.name()));
        }
    }

    @AfterClass
    public static void generateDocs() throws Exception {
        List<QuerySettings.QuerySettingDef<?>> settings = QuerySettings.SETTINGS_BY_NAME.values()
            .stream()
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

        DocsV3Support.SettingsTocDocsSupport toc = new DocsV3Support.SettingsTocDocsSupport(
            settings,
            QuerySettingsTests.class,
            DocsV3Support.callbacksFromSystemProperty()
        );
        toc.renderDocs();
    }
}
