/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomizeCase;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
        // An unknown SET key is a typo the user can act on, so it fails loudly — same as the request-body surface.
        String settingName = "non_existing";
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, settingName, of("12")));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        ParsingException e = expectThrows(ParsingException.class, () -> QuerySettings.validate(statement, SNAPSHOT_CTX_WITH_CPS_ENABLED));
        assertThat(e.getMessage(), containsString("Unknown setting [" + settingName + "]"));
    }

    public void testDeprecatedSettingWarnsButIsAccepted() {
        // A known-but-deprecated setting is not a typo: it keeps working and merely warns, so clients that
        // still send it are not broken (unlike an unknown key, which fails).
        QuerySettingDef<String> deprecated = QuerySettingDef.string("legacy_knob").withDeprecated("use new_knob instead").build();
        QuerySettings.warnIfDeprecated(deprecated);
        assertWarnings("Setting [legacy_knob] is deprecated: use new_knob instead");

        // A non-deprecated setting emits nothing.
        QuerySettings.warnIfDeprecated(QuerySettings.TIME_ZONE);
    }

    public void testCanonicalizeNormalizesOnOverride() {
        // TIME_ZONE.canonicalize(ZoneId::normalized) runs inside withOverride, so a programmatic (non-parsed)
        // value is normalized just like a parsed one — no caller has to remember to normalize.
        ResolvedSettings resolved = ResolvedSettings.EMPTY.withOverride(QuerySettings.TIME_ZONE, ZoneId.of("UTC"));
        assertThat(QuerySettings.TIME_ZONE.get(resolved), equalTo(ZoneId.of("UTC").normalized()));
        assertThat(QuerySettings.TIME_ZONE.get(resolved), not(equalTo(ZoneId.of("UTC"))));
    }

    public void testAllContainsEveryDeclaredSetting() throws IllegalAccessException {
        // Adding a QuerySettingDef constant but forgetting to add it to ALL compiles fine and fails only as an
        // "Unknown setting" at use — the one silent-miss in "add a setting". Guard it: every declared constant is in ALL.
        Set<QuerySettingDef<?>> declared = new HashSet<>();
        for (Field f : QuerySettings.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers()) && QuerySettingDef.class.isAssignableFrom(f.getType())) {
                declared.add((QuerySettingDef<?>) f.get(null));
            }
        }
        assertThat(Set.copyOf(QuerySettings.all()), equalTo(declared));
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

        // "UTC" is a fixed-offset zone, so it normalizes to ZoneOffset.UTC (see QuerySettings.parseZoneId).
        assertValid(setting, of("UTC"), equalTo(ZoneOffset.UTC));
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
        // "UTC" normalizes to ZoneOffset.UTC (see QuerySettings.parseZoneId).
        assertValid(setting, of("UTC"), equalTo(ZoneOffset.UTC), NON_SNAPSHOT_CTX_WITH_CPS_ENABLED);
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

    private static <T> void assertValid(QuerySettingDef<T> settingDef, Expression value, Matcher<T> parsedValueMatcher) {
        assertValid(settingDef, value, parsedValueMatcher, SNAPSHOT_CTX_WITH_CPS_ENABLED);
    }

    private static <T> void assertValid(
        QuerySettingDef<T> settingDef,
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

    private static <T> void assertDefault(QuerySettingDef<T> settingDef, Matcher<? super T> defaultMatcher) {
        EsqlStatement statement = new EsqlStatement(null, List.of());

        T value = statement.setting(settingDef);

        assertThat(value, defaultMatcher);
    }

    public void testResolveEmptySources() {
        ResolvedSettings resolved = QuerySettings.resolve(Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        // Default for time_zone is UTC
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneOffset.UTC));
        // Defaults for the rest are null
        assertThat(resolved.get(QuerySettings.PROJECT_ROUTING), is(nullValue()));
        assertThat(resolved.get(QuerySettings.APPROXIMATION), is(nullValue()));
    }

    public void testResolveBodyProjectRoutingFailsWithoutCps() {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.PROJECT_ROUTING, "my-project");
        var ex = expectThrows(
            VerificationException.class,
            () -> QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_DISABLED)
        );
        assertThat(ex.getMessage(), containsString("Error validating setting [project_routing]: cross-project search not enabled"));
    }

    public void testResolveRequestParameterAppliesWhenNoQuerySet() {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"));
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testResolveQuerySetOverridesRequestParameter() {
        // Request says Europe/Paris, query SET says UTC → query SET wins.
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"));
        QuerySetting set = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "time_zone", of("UTC")));
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, statement, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        // SET time_zone="UTC" wins and normalizes to ZoneOffset.UTC (see QuerySettings.parseZoneId).
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneOffset.UTC));
    }

    public void testBuildRejectsSnapshotAndServerlessOnly() {
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x").withSnapshotOnly().withServerlessOnly().build()
        );
        assertThat(e.getMessage(), containsString("cannot be both snapshotOnly and serverlessOnly"));
    }

    public void testBuildRejectsMissingStreamFormat() {
        // object(...) sets a JSON/expression reader but no stream format; build() must reject it.
        var e = expectThrows(IllegalStateException.class, () -> QuerySettingDef.object("x", p -> p.text(), ex -> null).build());
        assertThat(e.getMessage(), containsString("has no stream format"));
    }

    public void testBuildRejectsBodyExposedWithoutJsonReader() {
        // builder(name) has no JSON reader; opting into the request body without one is incoherent.
        var e = expectThrows(IllegalStateException.class, () -> QuerySettingDef.builder("x").withRequestBody().build());
        assertThat(e.getMessage(), containsString("body-exposed but has no JSON reader"));
    }

    public void testByNameRejectsDuplicateName() {
        QuerySettingDef<String> a = QuerySettingDef.string("dup").build();
        QuerySettingDef<String> b = QuerySettingDef.string("dup").build();
        var e = expectThrows(IllegalStateException.class, () -> QuerySettings.byName(List.of(a, b)));
        assertThat(e.getMessage(), containsString("Duplicate query setting [dup]"));
    }

    public void testUnknownSettingOnTheWireIsSkipped() throws IOException {
        // Simulate a newer peer sending a setting this node's registry doesn't have. The self-describing
        // (length-prefixed) format lets the reader skip it instead of failing. The unknown entry is placed FIRST so
        // that the known setting after it only parses correctly if the skip consumed exactly the unknown value's bytes.
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(2);

        out.writeString("a_future_setting_this_node_does_not_know");
        BytesStreamOutput unknownValue = new BytesStreamOutput();
        unknownValue.writeString("opaque");
        unknownValue.writeVInt(123); // arbitrary extra bytes, of a shape this node could not guess
        out.writeBytesReference(unknownValue.bytes());

        out.writeString(QuerySettings.TIME_ZONE.name());
        BytesStreamOutput knownValue = new BytesStreamOutput();
        QuerySettings.TIME_ZONE.writeValue(knownValue, ZoneId.of("Europe/Paris"));
        out.writeBytesReference(knownValue.bytes());

        ResolvedSettings resolved = new ResolvedSettings(out.bytes().streamInput());
        // The unknown one is silently skipped (no throw); the known setting after it survives intact.
        assertThat(QuerySettings.TIME_ZONE.get(resolved), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testResolveApproximationDisjointFieldsMerge() {
        // Request supplies rows only; query SET supplies confidence_level only. Both must survive.
        ApproximationSettings resolved = resolveApproximation(
            new ApproximationSettings(10000, 0.90),
            approxMap("confidence_level", Literal.fromDouble(Source.EMPTY, 0.92))
        );
        assertThat(resolved, is(new ApproximationSettings(10000, 0.92)));
    }

    public void testResolveApproximationSharedFieldSetWins() {
        // Both sources supply rows; query SET's value wins for the shared field.
        ApproximationSettings resolved = resolveApproximation(
            new ApproximationSettings(10000, 0.90),
            approxMap("rows", Literal.integer(Source.EMPTY, 50000), "confidence_level", Literal.fromDouble(Source.EMPTY, 0.85))
        );
        assertThat(resolved, is(new ApproximationSettings(50000, 0.85)));
    }

    public void testResolveApproximationRequestOnly() {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.APPROXIMATION, new ApproximationSettings(20000, 0.88));
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(QuerySettings.APPROXIMATION.get(resolved), is(new ApproximationSettings(20000, 0.88)));
    }

    public void testResolveApproximationSetOnly() {
        QuerySetting set = new QuerySetting(
            Source.EMPTY,
            new Alias(Source.EMPTY, "approximation", approxMap("rows", Literal.integer(Source.EMPTY, 30000)))
        );
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        ResolvedSettings settings = QuerySettings.resolve(Map.of(), statement, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        // SET supplied rows only; resolver enabled approximation and left confidence_level at its default.
        ApproximationSettings resolved = QuerySettings.APPROXIMATION.get(settings);
        assertThat(resolved.rows(), equalTo(30000));
    }

    public void testResolveApproximationBooleanTrueKeepsRequestFields() {
        // SET approximation=true parses to DEFAULT (rows=null, confidence_level=0.9). The field-level merge
        // treats null in the higher-precedence source as "no contribution for this field", so
        // request-supplied rows survive.
        ApproximationSettings resolved = resolveApproximation(
            new ApproximationSettings(50000, 0.85),
            Literal.fromBoolean(Source.EMPTY, true)
        );
        assertThat(resolved, is(new ApproximationSettings(50000, 0.9)));
    }

    public void testResolveApproximationBooleanFalseDisables() {
        // SET approximation=false parses to EXPLICIT_NULL, which disables approximation entirely
        // (Builder.merge with EXPLICIT_NULL clears the enabled flag → build() returns null).
        ApproximationSettings resolved = resolveApproximation(
            new ApproximationSettings(50000, 0.85),
            Literal.fromBoolean(Source.EMPTY, false)
        );
        assertThat(resolved, is(nullValue()));
    }

    private static ApproximationSettings resolveApproximation(ApproximationSettings requestValue, Expression querySetExpr) {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        if (requestValue != null) {
            requestParams.put(QuerySettings.APPROXIMATION, requestValue);
        }
        QuerySetting set = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "approximation", querySetExpr));
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        return QuerySettings.APPROXIMATION.get(QuerySettings.resolve(requestParams, statement, SNAPSHOT_CTX_WITH_CPS_ENABLED));
    }

    private static MapExpression approxMap(Object... kvs) {
        List<Expression> entries = new ArrayList<>();
        for (int i = 0; i < kvs.length; i += 2) {
            entries.add(Literal.keyword(Source.EMPTY, (String) kvs[i]));
            entries.add((Expression) kvs[i + 1]);
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    public void testResolveUnmappedFieldsIsSetOnly() {
        // UNMAPPED_FIELDS opted out of body exposure. The registry exposure flag is false.
        assertThat(QuerySettings.UNMAPPED_FIELDS.requestBody(), is(false));
        assertThat(QuerySettings.UNMAPPED_FIELDS.aliases().isEmpty(), is(true));
    }

    public void testResolveBodyExposedSettingsDeclareAliases() {
        // The three body-exposed settings each carry exactly one root alias mirroring the legacy field names.
        for (QuerySettingDef<?> def : List.of(QuerySettings.TIME_ZONE, QuerySettings.PROJECT_ROUTING, QuerySettings.APPROXIMATION)) {
            assertThat("requestParameterExposed for [" + def.name() + "]", def.requestBody(), is(true));
            assertThat("aliases for [" + def.name() + "]", def.aliases(), hasSize(1));
            QuerySettingDef.RequestBodyBinding alias = def.aliases().get(0);
            assertThat(alias.isAtRoot(), is(true));
            assertThat(alias.name(), equalTo(def.name()));
        }
    }

    @AfterClass
    public static void generateDocs() throws Exception {
        List<QuerySettingDef<?>> settings = QuerySettings.all().stream().sorted(Comparator.comparing(QuerySettingDef::name)).toList();

        for (QuerySettingDef<?> def : settings) {
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
