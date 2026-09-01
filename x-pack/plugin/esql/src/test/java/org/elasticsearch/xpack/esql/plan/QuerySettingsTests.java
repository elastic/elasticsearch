/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
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
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
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
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        String[] allValues = new String[] { "DEFAULT", "NULLIFY", "LOAD", "LOAD_ALL" };
        String[] nonSnapshotValues = new String[] { "DEFAULT", "NULLIFY", "LOAD" };

        assertDefault(setting, equalTo(UnmappedResolution.DEFAULT));

        for (String value : nonSnapshotValues) {
            assertValid(setting, of(randomizeCase(value)), equalTo(UnmappedResolution.valueOf(value)));
        }

        // LOAD_ALL is only valid on snapshot builds
        assertValid(setting, of(randomizeCase("LOAD_ALL")), equalTo(UnmappedResolution.LOAD_ALL), SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertValid(setting, of(randomizeCase("LOAD_ALL")), equalTo(UnmappedResolution.LOAD_ALL), SNAPSHOT_CTX_WITH_CPS_DISABLED);
        assertInvalid(
            setting.name(),
            NON_SNAPSHOT_CTX_WITH_CPS_ENABLED,
            of("LOAD_ALL"),
            "Error validating setting [unmapped_fields]: unmapped_fields value [LOAD_ALL] requires a snapshot build"
        );

        assertInvalid(setting.name(), of(12), "Setting [" + setting.name() + "] must be of type KEYWORD");

        // Parsing precedes the snapshot-only validator, so the values it lists come from the running build, not from the context.
        String[] parseErrorValues = Build.current().isSnapshot() ? allValues : nonSnapshotValues;
        for (SettingsValidationContext ctx : allSettingsValidationContexts) {
            assertInvalid(
                setting.name(),
                ctx,
                of("UNKNOWN"),
                "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of "
                    + Arrays.toString(parseErrorValues)
            );
        }

        Source settingSource = new Source(3, 10, "SET unmapped_fields = \"UNKNOWN\"");
        assertInvalidWithSource(
            setting.name(),
            settingSource,
            of("UNKNOWN"),
            "line 3:11: Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution [UNKNOWN], must be one of "
                + Arrays.toString(parseErrorValues)
        );
    }

    /**
     * Parsing happens before the snapshot-only validator runs, so the parse error is what a production build shows for a typo: it must
     * list only the values that build accepts. Tests always run on a snapshot build, hence the direct call with both flags.
     */
    public void testUnmappedFieldsParseErrorHidesSnapshotOnlyValue() {
        assertThat(
            QuerySettings.invalidUnmappedResolutionMessage("UNKNOWN", false),
            equalTo("Invalid unmapped_fields resolution [UNKNOWN], must be one of [DEFAULT, NULLIFY, LOAD]")
        );
        assertThat(
            QuerySettings.invalidUnmappedResolutionMessage("UNKNOWN", true),
            equalTo("Invalid unmapped_fields resolution [UNKNOWN], must be one of [DEFAULT, NULLIFY, LOAD, LOAD_ALL]")
        );
    }

    public void testValidate_ColumnMetadata() {
        var setting = QuerySettings.COLUMN_METADATA;

        assertDefault(setting, equalTo(Boolean.FALSE));

        assertValid(setting, Literal.fromBoolean(Source.EMPTY, true), equalTo(Boolean.TRUE));
        assertValid(setting, Literal.fromBoolean(Source.EMPTY, false), equalTo(Boolean.FALSE));

        assertInvalid(setting.name(), of("true"), "Setting [" + setting.name() + "] must be of type BOOLEAN");
        assertInvalid(setting.name(), Literal.integer(Source.EMPTY, 1), "Setting [" + setting.name() + "] must be of type BOOLEAN");
        assertInvalid(
            setting.name(),
            new MapExpression(Source.EMPTY, List.of()),
            "Setting [" + setting.name() + "] must be of type BOOLEAN"
        );
        assertInvalid(
            setting.name(),
            new Literal(Source.EMPTY, List.of(true, false), DataType.BOOLEAN),
            "Setting [" + setting.name() + "] must be a boolean"
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

    public void testResolveColumnMetadataIsRequestBodyExposedWithoutAlias() {
        // COLUMN_METADATA is body-exposed under settings.{} but, unlike the three legacy settings, carries no
        // top-level alias — there was never a pre-existing top-level body field for it to stay compatible with.
        assertThat(QuerySettings.COLUMN_METADATA.requestBody(), is(true));
        assertThat(QuerySettings.COLUMN_METADATA.aliases().isEmpty(), is(true));
    }

    public void testResolveColumnMetadataDefault() {
        // Nothing supplied it anywhere (no body, no SET) — the registered default applies.
        ResolvedSettings resolved = QuerySettings.resolve(Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.FALSE));
    }

    public void testResolveRequestParameterAppliesColumnMetadata() {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.COLUMN_METADATA, Boolean.TRUE);
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.TRUE));
    }

    public void testResolveRequestParameterAppliesColumnMetadataExplicitFalse() {
        // Explicit false is a real, user-supplied value — distinct from "not supplied" even though both
        // resolve to the same FALSE default. Guards against a reconciler/resolver that mistakes a falsy
        // value for an absent one (e.g. an accidental truthiness check instead of a null check).
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.COLUMN_METADATA, Boolean.FALSE);
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.FALSE));
    }

    public void testResolveColumnMetadataRejectsMalformedSetValue() {
        // resolve() calls readFromExpression() directly and does not repeat validate()'s upfront type check.
        // This confirms the bool() factory's own defensive check rejects a non-boolean SET value on its own,
        // so a malformed value can't silently slip through resolve() even if validate() were ever bypassed.
        QuerySetting setting = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "column_metadata", Literal.integer(Source.EMPTY, 1)));
        EsqlStatement statement = new EsqlStatement(null, List.of(setting));
        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> QuerySettings.resolve(Map.of(), statement, SNAPSHOT_CTX_WITH_CPS_ENABLED)
        );
        assertThat(ex.getMessage(), containsString("Setting [column_metadata] must be a boolean, got [1]"));
    }

    public void testResolveBodyColumnMetadataOnNonSnapshot() {
        // column_metadata is de-snapshotted (#148508): body-supplied values resolve on release builds too.
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.COLUMN_METADATA, Boolean.TRUE);
        ResolvedSettings resolved = QuerySettings.resolve(requestParams, null, NON_SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.TRUE));
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

    // ---- Cluster (operator) defaults: default < cluster < body < SET ----

    private static Settings clusterSetting(QuerySettingDef<?> def, String value) {
        return Settings.builder().put(QuerySettingDef.CLUSTER_SETTING_PREFIX + def.name(), value).build();
    }

    public void testClusterSettingsDerivedOnlyForOptedInSettings() {
        Set<String> derivedKeys = new HashSet<>();
        for (Setting<?> setting : QuerySettings.clusterSettings()) {
            derivedKeys.add(setting.getKey());
        }
        assertThat(derivedKeys, equalTo(Set.of("esql.query.settings.time_zone", "esql.query.settings.unmapped_fields")));

        // A setting that did not opt in has no key at all, so the key stays unknown and is rejected as a typo
        // rather than silently accepted.
        assertThat(QuerySettings.COLUMN_METADATA.clusterSetting(), is(nullValue()));
        assertThat(QuerySettings.APPROXIMATION.clusterSetting(), is(nullValue()));
        assertThat(QuerySettings.PROJECT_ROUTING.clusterSetting(), is(nullValue()));
    }

    public void testDerivedSettingsAreRegisteredByThePlugin() {
        // Without this the documented keys are unwritable — PUT _cluster/settings rejects them as unknown and a node
        // carrying one in elasticsearch.yml refuses to start — while every other test still passes.
        List<Setting<?>> pluginSettings = new EsqlPlugin().getSettings();
        for (Setting<?> derived : QuerySettings.clusterSettings()) {
            assertThat(pluginSettings, hasItem(derived));
        }
    }

    public void testNodeSettingsSupplyTheDefaultWhenClusterStateHasNone() {
        // The elasticsearch.yml path. Both sources are read directly, so a yml value applies with no cluster state.
        ResolvedSettings resolved = QuerySettings.resolve(
            Settings.EMPTY,
            clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"),
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testClusterStateWinsOverNodeSettings() {
        // The precedence AbstractScopedSettings itself applies when it merges the two.
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.TIME_ZONE, "Asia/Tokyo"),
            clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"),
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Asia/Tokyo")));
    }

    public void testUnusableValueIsReportedThroughTheRegisteredUpdateConsumer() {
        // Drives the real settings-update path, so it pins the registration and not just the method body. That
        // matters more than usual here: the fallback is silent by design, so a registration that never happened has
        // no symptom at all. An integration test cannot cover this — PUT _cluster/settings refuses any value that
        // would warn, which is the whole point of the write-time check.
        Settings node = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(node, new HashSet<>(QuerySettings.clusterSettings()));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(node);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        QuerySettings.watchClusterDefaults(clusterService);

        String key = QuerySettingDef.CLUSTER_SETTING_PREFIX + QuerySettings.UNMAPPED_FIELDS.name();
        MockLog.assertThatLogger(
            () -> clusterSettings.applySettings(Settings.builder().put(key, "not_a_resolution").build()),
            QuerySettings.class,
            new MockLog.SeenEventExpectation(
                "unusable operator value",
                QuerySettings.class.getCanonicalName(),
                Level.WARN,
                "*" + key + "*not usable*fall back*Invalid unmapped_fields resolution*"
            )
        );
    }

    public void testUsableAndAbsentValuesAreNotReported() {
        MockLog.assertThatLogger(
            () -> QuerySettings.warnUnusableClusterDefaults(clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris")),
            QuerySettings.class,
            new MockLog.UnseenEventExpectation("no warning", QuerySettings.class.getCanonicalName(), Level.WARN, "*")
        );
        MockLog.assertThatLogger(
            () -> QuerySettings.warnUnusableClusterDefaults(Settings.EMPTY),
            QuerySettings.class,
            new MockLog.UnseenEventExpectation("no warning", QuerySettings.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    public void testDerivedClusterSettingIsDynamicAndNodeScoped() {
        for (Setting<?> setting : QuerySettings.clusterSettings()) {
            assertThat(setting.getKey(), setting.isDynamic(), is(true));
            assertThat(setting.getKey(), setting.hasNodeScope(), is(true));
        }
    }

    public void testClusterSettingDeclaredDefaultIsTheRegistryDefault() {
        // There is one default in the system. The derived setting declares the registry default as its own so
        // include_defaults reports the value queries actually get — it is not a second, independent default.
        assertThat(QuerySettings.TIME_ZONE.clusterSetting().get(Settings.EMPTY), equalTo(ZoneOffset.UTC));
        assertThat(QuerySettings.TIME_ZONE.clusterSetting().get(Settings.EMPTY), equalTo(QuerySettings.TIME_ZONE.defaultValue()));
        assertThat(
            QuerySettings.UNMAPPED_FIELDS.clusterSetting().get(Settings.EMPTY),
            equalTo(QuerySettings.UNMAPPED_FIELDS.defaultValue())
        );
    }

    public void testClusterDefaultAppliesWhenNoPerQuerySource() {
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"),
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testRequestBodyOverridesClusterDefault() {
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Asia/Tokyo"));
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"),
            Settings.EMPTY,
            requestParams,
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Asia/Tokyo")));
    }

    public void testQuerySetOverridesClusterDefaultAndBody() {
        // The full chain in one query: operator says Paris, the calling application says Tokyo, the query author
        // says Berlin. The narrowest scope of authority wins.
        Map<QuerySettingDef<?>, Object> requestParams = new HashMap<>();
        requestParams.put(QuerySettings.TIME_ZONE, ZoneId.of("Asia/Tokyo"));
        QuerySetting set = new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "time_zone", of("Europe/Berlin")));
        EsqlStatement statement = new EsqlStatement(null, List.of(set));
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"),
            Settings.EMPTY,
            requestParams,
            statement,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Berlin")));
    }

    public void testOperatorValueReplacesTheDefaultRatherThanReconcilingWithIt() {
        // An operator changes what the default IS, so their value substitutes for the registry default rather than
        // reconciling with it. The difference is only visible through a reconciler that combines rather than replaces:
        // both opted-in settings are last-wins scalars, for which substituting and reconciling agree, so neither can
        // pin this. A concatenating reconciler separates them — merging would give "d+op".
        QuerySettingDef<String> merging = QuerySettingDef.string("merging")
            .withDefault("d")
            .withReconciler((previous, current) -> previous == null ? current : previous + "+" + current)
            .withClusterDefault()
            .build();

        ResolvedSettings unset = QuerySettings.resolve(
            List.of(merging),
            Settings.EMPTY,
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat("an unset key must leave the registry default alone", unset.get(merging), equalTo("d"));

        ResolvedSettings set = QuerySettings.resolve(
            List.of(merging),
            Settings.builder().put(merging.clusterSetting().getKey(), "op").build(),
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        // The operator changes what the default IS. Reconciling it with the registry default would merge the two —
        // "d+op" — which is what the earlier shape did and is wrong: there is one default, and this replaces it.
        assertThat("a set key must replace the registry default, not merge with it", set.get(merging), equalTo("op"));
    }

    public void testUnsetClusterLayerLeavesResolutionUnchanged() {
        ResolvedSettings withEmptyCluster = QuerySettings.resolve(
            Settings.EMPTY,
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        ResolvedSettings withoutClusterLayer = QuerySettings.resolve(Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(withEmptyCluster, equalTo(withoutClusterLayer));

        // An unrelated key present in the same Settings must not be mistaken for one of ours.
        Settings unrelated = Settings.builder().put("esql.query.allow_partial_results", false).build();
        assertThat(
            QuerySettings.resolve(unrelated, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED),
            equalTo(withoutClusterLayer)
        );
    }

    public void testSettingWithoutClusterDefaultIgnoresItsWouldBeKey() {
        // column_metadata did not opt in, so even a value sitting at its would-be key changes nothing.
        Settings stray = Settings.builder().put("esql.query.settings.column_metadata", true).build();
        ResolvedSettings resolved = QuerySettings.resolve(stray, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.FALSE));
        assertThat(resolved, equalTo(QuerySettings.resolve(Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED)));
    }

    public void testSnapshotOnlyOperatorValueDoesNotEscapeOntoAReleaseBuild() {
        // unmapped_fields=LOAD_ALL is snapshot-only, enforced by the setting's own validator. Written on a snapshot
        // build the value persists; if the cluster later runs a release build it must stop applying rather than
        // silently remain in force. Whether it is refused depends on the build this test runs on, which is what the
        // write-time context reports, so assert against that rather than hardcoding one build type.
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.UNMAPPED_FIELDS, "LOAD_ALL"),
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        UnmappedResolution expected = Build.current().isSnapshot() ? UnmappedResolution.LOAD_ALL : UnmappedResolution.DEFAULT;
        assertThat(resolved.get(QuerySettings.UNMAPPED_FIELDS), equalTo(expected));
    }

    public void testDriftedOperatorValueFallsBackToTheRegistryDefault() {
        // A value that was valid when the operator wrote it and is not any more — the environment drifted, or the
        // cluster restarted onto a build where it is no longer allowed. Two things must both hold: the query must not
        // fail (the operator is not in the request path, so failing punishes users who cannot fix it), and the value
        // must not stay in force either (it is no longer permitted). So it falls back to the registry default.
        QuerySettingDef<String> def = QuerySettingDef.string("drifted_operator_value")
            .withDefault("ok")
            .withValidator((value, ctx) -> value.equals("drifted") ? "no longer valid in this environment" : null)
            .withClusterDefault()
            .build();
        Settings drifted = Settings.builder().put(def.clusterSetting().getKey(), "drifted").build();

        ResolvedSettings resolved = QuerySettings.resolve(
            List.of(def),
            drifted,
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(def), equalTo("ok"));

        // ... and the operator is told, on their own channel rather than in every query.
        assertThat(def.clusterValueError(drifted, Settings.EMPTY), equalTo("no longer valid in this environment"));
    }

    public void testUnparseableOperatorValueFallsBackToTheRegistryDefault() {
        // Rejected when written, so this is only reachable by drift — a stored raw that a later build no longer
        // parses. It must not throw out of resolution.
        QuerySettingDef<String> def = QuerySettingDef.string("strict_parse", value -> {
            if (value.equals("bad")) {
                throw new IllegalArgumentException("cannot parse [bad]");
            }
            return value;
        }).withDefault("ok").withClusterDefault().build();
        Settings bad = Settings.builder().put(def.clusterSetting().getKey(), "bad").build();

        ResolvedSettings resolved = QuerySettings.resolve(List.of(def), bad, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED);
        assertThat(resolved.get(def), equalTo("ok"));
        assertThat(def.clusterValueError(bad, Settings.EMPTY), containsString("cannot parse [bad]"));
    }

    public void testValidatorThrowingAtResolveTimeFallsBackAndIsReported() {
        // Distinct from a validator that returns an error string: one that throws must not escape onto the query path
        // either, and must still be reported rather than read as usable.
        QuerySettingDef<String> def = QuerySettingDef.string("throwing_validator").withDefault("ok").withValidator((value, ctx) -> {
            if (value.equals("boom")) {
                throw new IllegalStateException("validator blew up");
            }
            return null;
        }).withClusterDefault().build();
        Settings boom = Settings.builder().put(def.clusterSetting().getKey(), "boom").build();

        assertThat(
            QuerySettings.resolve(List.of(def), boom, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED).get(def),
            equalTo("ok")
        );
        assertThat(def.clusterValueError(boom, Settings.EMPTY), equalTo("validator blew up"));
    }

    public void testCanonicalizerRejectingTheOperatorValueFallsBack() {
        // canonicalize runs on every write into the resolved view, so a canonicalizer that rejects an operator value
        // would otherwise throw on the query path — the one route left by which an operator could break every query.
        QuerySettingDef<String> def = QuerySettingDef.string("picky_canonicalizer").withDefault("ok").canonicalize(value -> {
            if (value.equals("unrepresentable")) {
                throw new IllegalArgumentException("cannot canonicalize");
            }
            return value;
        }).withClusterDefault().build();
        Settings bad = Settings.builder().put(def.clusterSetting().getKey(), "unrepresentable").build();

        assertThat(
            QuerySettings.resolve(List.of(def), bad, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED).get(def),
            equalTo("ok")
        );
    }

    public void testErrorIsReportedForAnExceptionCarryingNoMessage() {
        // A null message must not read as "usable" — that would be a silent fallback with no operator signal. An
        // anonymous throwable additionally has an empty simple name, which would put us back in the same place.
        QuerySettingDef<String> def = QuerySettingDef.<String>string("silent_thrower", value -> {
            if (value.equals("whatever")) {
                throw new IllegalArgumentException() {};
            }
            return value;
        }).withDefault("ok").withClusterDefault().build();
        Settings any = Settings.builder().put(def.clusterSetting().getKey(), "whatever").build();

        String error = def.clusterValueError(any, Settings.EMPTY);
        assertThat(error, is(notNullValue()));
        assertThat(error, is(not(emptyString())));
        assertThat(
            QuerySettings.resolve(List.of(def), any, Settings.EMPTY, Map.of(), null, SNAPSHOT_CTX_WITH_CPS_ENABLED).get(def),
            equalTo("ok")
        );
    }

    public void testUsableOperatorValueReportsNoError() {
        assertThat(
            QuerySettings.TIME_ZONE.clusterValueError(clusterSetting(QuerySettings.TIME_ZONE, "Europe/Paris"), Settings.EMPTY),
            is(nullValue())
        );
        assertThat(QuerySettings.TIME_ZONE.clusterValueError(Settings.EMPTY, Settings.EMPTY), is(nullValue()));
    }

    public void testBuildRejectsClusterDefaultWhoseDefaultDoesNotParse() {
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x", value -> { throw new IllegalArgumentException("nope"); })
                .withDefault("v")
                .withClusterDefault()
                .build()
        );
        assertThat(e.getMessage(), containsString("does not parse back"));
    }

    public void testBuildRejectsClusterDefaultWhoseValidatorThrowsOnItsDefault() {
        var e = expectThrows(IllegalStateException.class, () -> QuerySettingDef.string("x").withDefault("v").withValidator((value, ctx) -> {
            throw new IllegalStateException("validator blew up");
        }).withClusterDefault().build());
        assertThat(e.getMessage(), containsString("its own validator throws on its default"));
    }

    public void testBuildRejectsClusterDefaultWhoseDefaultFailsItsOwnValidator() {
        // Setting#get validates whatever it returns, the declared default included, so include_defaults would throw.
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x").withDefault("bad").withValidator((value, ctx) -> "always fails").withClusterDefault().build()
        );
        assertThat(e.getMessage(), containsString("its own validator rejects its default"));
    }

    public void testUnmappedFieldsIsClusterSettableButNotBodyExposed() {
        // The sources are independent axes, not a ladder: unmapped_fields is SET-only on the request side and still
        // takes an operator default.
        assertThat(QuerySettings.UNMAPPED_FIELDS.requestBody(), is(false));
        assertNotNull(QuerySettings.UNMAPPED_FIELDS.clusterSetting());
        ResolvedSettings resolved = QuerySettings.resolve(
            clusterSetting(QuerySettings.UNMAPPED_FIELDS, "NULLIFY"),
            Settings.EMPTY,
            Map.of(),
            null,
            SNAPSHOT_CTX_WITH_CPS_ENABLED
        );
        assertThat(resolved.get(QuerySettings.UNMAPPED_FIELDS), equalTo(UnmappedResolution.NULLIFY));
    }

    public void testDerivedClusterSettingRejectsMalformedValueAtWriteTime() {
        // The derived parser is the same FromString the SET and body surfaces use, so the two cannot drift.
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> QuerySettings.TIME_ZONE.clusterSetting().get(clusterSetting(QuerySettings.TIME_ZONE, "Not/AZone"))
        );
        assertThat(e.getMessage(), containsString("Invalid time zone [Not/AZone]"));
    }

    public void testDerivedClusterSettingRunsTheDeclaredValidatorAtWriteTime() {
        QuerySettingDef<String> def = QuerySettingDef.string("write_time_validated")
            .withDefault("ok")
            .withValidator((value, ctx) -> value.equals("bad") ? "value [bad] is not allowed" : null)
            .withClusterDefault()
            .build();
        // The default still resolves.
        assertThat(def.clusterSetting().get(Settings.EMPTY), equalTo("ok"));
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> def.clusterSetting().get(Settings.builder().put(def.clusterSetting().getKey(), "bad").build())
        );
        assertThat(e.getMessage(), containsString("value [bad] is not allowed"));
    }

    public void testBuildRejectsClusterDefaultWithoutRegistryDefault() {
        var e = expectThrows(IllegalStateException.class, () -> QuerySettingDef.string("x").withClusterDefault().build());
        assertThat(e.getMessage(), containsString("it has no registry default"));
    }

    public void testBuildRejectsClusterDefaultWithoutStringForm() {
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.object("x", p -> p.text(), ex -> "v")
                .withDefault("v")
                .streamFormat((out, value) -> out.writeString(value), in -> in.readString())
                .withClusterDefault()
                .build()
        );
        assertThat(e.getMessage(), containsString("its value has no string form"));
    }

    public void testBuildRejectsClusterDefaultOnServerlessOnly() {
        // serverlessOnly marks the settings whose validator reads environment we cannot evaluate when an operator
        // writes the key, so the placeholder cross-project flag in that context can never be reached.
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x").withDefault("v").withServerlessOnly().withClusterDefault().build()
        );
        assertThat(e.getMessage(), containsString("a serverlessOnly setting's validator reads environment"));
    }

    public void testBuildRejectsClusterDefaultOnSnapshotOnly() {
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x").withDefault("v").withSnapshotOnly().withClusterDefault().build()
        );
        assertThat(e.getMessage(), containsString("must not register a permanent public cluster key"));
    }

    public void testBuildRejectsClusterDefaultWhoseDefaultDoesNotRoundTrip() {
        // The declared default is the registry default rendered with toString(); a default that cannot survive that
        // round trip would make include_defaults report a different value than the one actually in force.
        var e = expectThrows(
            IllegalStateException.class,
            () -> QuerySettingDef.string("x", v -> "always-this").withDefault("something-else").withClusterDefault().build()
        );
        assertThat(e.getMessage(), containsString("does not round-trip through its own parser"));
    }

}
