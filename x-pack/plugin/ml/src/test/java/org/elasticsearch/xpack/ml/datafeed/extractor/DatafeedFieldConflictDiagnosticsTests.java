/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class DatafeedFieldConflictDiagnosticsTests extends ESTestCase {

    public void testIntegralWideningAcrossProjectsIsCompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("long", "integer")), is(true));
    }

    public void testFloatingWideningAcrossProjectsIsCompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("double", "float")), is(true));
    }

    public void testDateAndDateNanosAcrossProjectsIsCompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("date", "date_nanos")), is(true));
    }

    public void testKeywordSubtypesAcrossProjectsIsCompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("keyword", "constant_keyword")), is(true));
    }

    public void testKeywordAndTextAcrossProjectsIsIncompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("keyword", "text")), is(false));
    }

    public void testIpAndKeywordAcrossProjectsIsIncompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("ip", "keyword")), is(false));
    }

    public void testBooleanAndKeywordAcrossProjectsIsIncompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("boolean", "keyword")), is(false));
    }

    public void testLongAndDoubleAcrossProjectsIsIncompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("long", "double")), is(false));
    }

    public void testDetectIncompatibleGroupsProjectsAndOrigin() {
        FieldCapabilitiesResponse response = responseFor(
            "status",
            Map.of("keyword", fieldCaps("status", "keyword", "prod-us:logs-*"), "long", fieldCaps("status", "long", "logs-*"))
        );

        List<DatafeedFieldConflictDiagnostics.FieldTypeConflict> conflicts = DatafeedFieldConflictDiagnostics.detectIncompatible(
            response,
            List.of("status")
        );

        assertThat(conflicts, hasSize(1));
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflicts.get(0);
        assertThat(conflict.field(), equalTo("status"));
        assertThat(conflict.typeToProjects().get("keyword"), equalTo(Set.of("prod-us")));
        assertThat(conflict.typeToProjects().get("long"), equalTo(Set.of("_origin")));
        assertThat(DatafeedFieldConflictDiagnostics.isIncompatibleOptionalField(conflict), is(true));
    }

    public void testTimeFieldDateAndDateNanosIsNotIncompatible() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("timestamp", "date", "prod-us", "date_nanos", "prod-eu");
        assertThat(DatafeedFieldConflictDiagnostics.isIncompatibleTimeField(conflict), is(false));
    }

    public void testTimeFieldDateAndLongIsIncompatible() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("@timestamp", "date", "prod-us", "long", "prod-eu");
        assertThat(DatafeedFieldConflictDiagnostics.isIncompatibleTimeField(conflict), is(true));
    }

    public void testOptionalFieldWarningUsesReportOnlyTemplate() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("status", "keyword", "prod-us", "long", "prod-eu");
        String message = DatafeedFieldConflictDiagnostics.optionalFieldWarning("my-datafeed", conflict);
        assertThat(
            message,
            equalTo(
                Messages.getMessage(
                    Messages.JOB_AUDIT_DATAFEED_FIELD_TYPE_CONFLICT,
                    "my-datafeed",
                    "status",
                    "keyword in [prod-us], long in [prod-eu]"
                )
            )
        );
        assertThat(message, containsString("status"));
        assertThat(message.contains("excluded"), is(false));
    }

    public void testTimeFieldConflictErrorTemplate() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("@timestamp", "date", "prod-us", "long", "prod-eu");
        String message = DatafeedFieldConflictDiagnostics.timeFieldConflictError("my-datafeed", "@timestamp", conflict);
        assertThat(message, containsString("Cannot run datafeed [my-datafeed]"));
        assertThat(message, containsString("@timestamp"));
    }

    public void testTimeFieldProjectExcludedErrorTemplate() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("@timestamp", "date", "prod-us", "long", "prod-eu");
        String message = DatafeedFieldConflictDiagnostics.timeFieldProjectExcludedError("my-datafeed", "@timestamp", "prod-eu", conflict);
        assertThat(message, containsString("excluded project [prod-eu]"));
        assertThat(message, containsString("@timestamp"));
    }

    public void testUnlistedOptionalFieldTypeIsCompatible() {
        assertThat(DatafeedFieldConflictDiagnostics.areOptionalFieldTypesCompatible(Set.of("long", "unsigned_long")), is(true));
    }

    public void testOptionalFieldCompatibilityGroupsHaveNoOverlappingTypes() {
        Set<String> seen = new java.util.HashSet<>();
        for (Set<String> group : DatafeedFieldConflictDiagnostics.optionalFieldCompatibilityGroups()) {
            for (String type : group) {
                assertThat("type [" + type + "] appears in more than one compatibility group", seen.add(type), is(true));
            }
        }
    }

    public void testOptionalFieldCompatibilityGroupsCoverMlRelevantTypes() {
        Set<String> expected = Set.of(
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "scaled_float",
            "date",
            "date_nanos",
            "keyword",
            "constant_keyword",
            "wildcard",
            "text",
            "match_only_text",
            "boolean",
            "ip",
            "geo_point",
            "geo_shape",
            "object",
            "nested"
        );
        Set<String> covered = new java.util.HashSet<>();
        for (Set<String> group : DatafeedFieldConflictDiagnostics.optionalFieldCompatibilityGroups()) {
            covered.addAll(group);
        }
        assertThat(covered, equalTo(expected));
    }

    public void testRemoveProjectsClearsConflictWhenOnlyOneTypeRemains() {
        DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict = conflict("status", "keyword", "prod-us", "long", "prod-eu");
        assertThat(DatafeedFieldConflictDiagnostics.removeProjects(conflict, Set.of("prod-eu")), equalTo(null));
    }

    private static DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict(
        String field,
        String type1,
        String project1,
        String type2,
        String project2
    ) {
        FieldCapabilitiesResponse response = responseFor(
            field,
            Map.of(type1, fieldCaps(field, type1, project1 + ":logs-*"), type2, fieldCaps(field, type2, project2 + ":logs-*"))
        );
        return DatafeedFieldConflictDiagnostics.detectIncompatible(response, List.of(field)).get(0);
    }

    private static FieldCapabilitiesResponse responseFor(String field, Map<String, FieldCapabilities> typeToCaps) {
        return new FieldCapabilitiesResponse(new String[] { "logs-*" }, Map.of(field, typeToCaps));
    }

    private static FieldCapabilities fieldCaps(String field, String type, String index) {
        return new FieldCapabilities(
            field,
            type,
            false,
            true,
            true,
            null,
            false,
            null,
            new String[] { index },
            null,
            null,
            null,
            null,
            null,
            Map.of()
        );
    }
}
