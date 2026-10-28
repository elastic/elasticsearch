/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * The settings view definitions {@code SET} for their own bodies reach APM on their own counters, apart from the caller's:
 * a setting counts once per query on {@code queries.total} and once per view that sets it on {@code usages.total}, and each
 * value a view body resolved it to counts once per query, whether the definition {@code SET} it or left it at its default.
 */
public class PlanTelemetryViewSettingsTests extends ESTestCase {

    private static PlanTelemetry telemetry() {
        return new PlanTelemetry(new EsqlFunctionRegistry(), QuerySettings.applicableIn(true, true));
    }

    public void testViewSettingsAreRecordedApartFromTheCallers() {
        PlanTelemetry telemetry = telemetry();
        telemetry.setting("unmapped_fields");
        telemetry.resolvedSettings(ResolvedSettings.EMPTY);
        telemetry.viewSettings(
            List.of(
                new ViewQuerySettings(UnmappedResolution.NULLIFY, false, false, Set.of("unmapped_fields")),
                new ViewQuerySettings(UnmappedResolution.NULLIFY, true, false, Set.of("unmapped_fields", "wildcards_match_views")),
                ViewQuerySettings.DEFAULTS
            )
        );

        assertThat(telemetry.viewSettings(), equalTo(Map.of("UNMAPPED_FIELDS", 2, "WILDCARDS_MATCH_VIEWS", 1)));
        // every view's resolved values, defaults included, even for the view that SETs nothing
        assertThat(
            telemetry.viewResolvedSettings(),
            equalTo(
                Map.of(
                    "UNMAPPED_FIELDS",
                    Set.of("default", "nullify"),
                    "WILDCARDS_MATCH_VIEWS",
                    Set.of("false", "true"),
                    "WILDCARDS_MATCH_DATASETS",
                    Set.of("false")
                )
            )
        );
        // the caller's own settings are what the caller set, whatever the views set
        assertThat(telemetry.settings(), equalTo(Map.of("UNMAPPED_FIELDS", 1)));
        assertThat(telemetry.resolvedSettings().get("UNMAPPED_FIELDS"), equalTo("default"));
    }

    public void testNoViewsRecordNothing() {
        PlanTelemetry telemetry = telemetry();
        telemetry.viewSettings(List.of());
        assertThat(telemetry.viewSettings(), anEmptyMap());
        assertThat(telemetry.viewResolvedSettings(), anEmptyMap());
    }

    public void testViewsThatSetNothingRecordOnlyDefaults() {
        PlanTelemetry telemetry = telemetry();
        telemetry.viewSettings(List.of(ViewQuerySettings.DEFAULTS, ViewQuerySettings.DEFAULTS));
        assertThat(telemetry.viewSettings(), anEmptyMap());
        assertThat(
            telemetry.viewResolvedSettings(),
            equalTo(
                Map.of(
                    "UNMAPPED_FIELDS",
                    Set.of("default"),
                    "WILDCARDS_MATCH_VIEWS",
                    Set.of("false"),
                    "WILDCARDS_MATCH_DATASETS",
                    Set.of("false")
                )
            )
        );
    }

    public void testPublish() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        PlanTelemetry telemetry = telemetry();
        telemetry.viewSettings(
            List.of(
                new ViewQuerySettings(UnmappedResolution.NULLIFY, false, false, Set.of("unmapped_fields")),
                new ViewQuerySettings(UnmappedResolution.DEFAULT, false, false, Set.of("unmapped_fields"))
            )
        );
        boolean success = randomBoolean();
        manager.publish(telemetry, success);

        List<Measurement> queries = measurements(registry, PlanTelemetryManager.VIEW_SETTING_METRICS);
        assertThat(queries.size(), equalTo(1));
        assertThat(queries.get(0).getLong(), equalTo(1L));
        assertThat(queries.get(0).attributes(), equalTo(Map.of("feature_name", "UNMAPPED_FIELDS", "success", success)));

        List<Measurement> usages = measurements(registry, PlanTelemetryManager.VIEW_SETTING_METRICS_ALL);
        assertThat(usages.size(), equalTo(1));
        assertThat(usages.get(0).getLong(), equalTo(2L));

        Map<String, Long> resolved = new HashMap<>();
        for (Measurement m : measurements(registry, PlanTelemetryManager.VIEW_RESOLVED_SETTINGS_METRICS)) {
            assertThat(m.attributes().get("success"), equalTo(success));
            resolved.merge(m.attributes().get("feature_name") + "." + m.attributes().get("setting_value"), m.getLong(), Long::sum);
        }
        assertThat(
            resolved,
            equalTo(
                Map.of(
                    "UNMAPPED_FIELDS.nullify",
                    1L,
                    "UNMAPPED_FIELDS.default",
                    1L,
                    "WILDCARDS_MATCH_VIEWS.false",
                    1L,
                    "WILDCARDS_MATCH_DATASETS.false",
                    1L
                )
            )
        );

        // nothing reached the caller's counters
        assertThat(measurements(registry, PlanTelemetryManager.SETTING_METRICS), empty());
        assertThat(measurements(registry, PlanTelemetryManager.RESOLVED_SETTINGS_METRICS), empty());
    }

    private static List<Measurement> measurements(RecordingMeterRegistry registry, String name) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
    }
}
