/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.extractor.GeoPointField;
import org.elasticsearch.xpack.ml.extractor.GeoShapeField;
import org.elasticsearch.xpack.ml.extractor.TimeField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Actionable user-facing messages when cross-project field-mapping conflicts are detected in CPS datafeeds.
 */
public final class DatafeedFieldConflictDiagnostics {

    private static final String ORIGIN_PROJECT = "_origin";

    private static final Set<String> INTEGRAL_TYPES = Set.of("long", "integer", "short", "byte");
    private static final Set<String> FLOATING_TYPES = Set.of("double", "float", "half_float", "scaled_float");
    private static final Set<String> KEYWORD_TYPES = Set.of("keyword", "constant_keyword", "wildcard");
    private static final Set<String> TEXT_TYPES = Set.of("text", "match_only_text");
    private static final Set<String> BOOLEAN_TYPES = Set.of("boolean");
    private static final Set<String> IP_TYPES = Set.of("ip");
    private static final Set<String> GEO_POINT_TYPES = Set.of(GeoPointField.TYPE);
    private static final Set<String> GEO_SHAPE_TYPES = Set.of(GeoShapeField.TYPE);
    private static final Set<String> OBJECT_TYPES = Set.of("object", "nested");

    public record FieldTypeConflict(String field, SortedMap<String, SortedSet<String>> typeToProjects) {}

    private DatafeedFieldConflictDiagnostics() {}

    private static final List<Set<String>> OPTIONAL_FIELD_COMPATIBILITY_GROUPS = List.of(
        INTEGRAL_TYPES,
        FLOATING_TYPES,
        TimeField.TYPES,
        KEYWORD_TYPES,
        TEXT_TYPES,
        BOOLEAN_TYPES,
        IP_TYPES,
        GEO_POINT_TYPES,
        GEO_SHAPE_TYPES,
        OBJECT_TYPES
    );

    /**
     * Returns the analytical type-family groups used to judge optional-field compatibility across projects.
     * Exposed for drift-guard unit tests.
     */
    static List<Set<String>> optionalFieldCompatibilityGroups() {
        return OPTIONAL_FIELD_COMPATIBILITY_GROUPS;
    }

    /**
     * Returns {@code true} when the union of Elasticsearch types across projects is analytically compatible
     * for optional fields. This is a report-only judgement: some multi-type unions (for example {@code keyword}
     * and {@code text}) still collapse to a single extraction path via
     * {@link org.elasticsearch.xpack.ml.extractor.ExtractedFields.ExtractionMethodDetector},
     * but we warn when types span distinct analytical families.
     */
    public static boolean areOptionalFieldTypesCompatible(Set<String> types) {
        if (types.size() <= 1) {
            return true;
        }
        boolean allTypesKnown = true;
        for (String type : types) {
            if (isKnownOptionalFieldType(type) == false) {
                allTypesKnown = false;
                break;
            }
        }
        if (allTypesKnown == false) {
            return true;
        }
        for (Set<String> group : OPTIONAL_FIELD_COMPATIBILITY_GROUPS) {
            if (group.containsAll(types)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isKnownOptionalFieldType(String type) {
        for (Set<String> group : OPTIONAL_FIELD_COMPATIBILITY_GROUPS) {
            if (group.contains(type)) {
                return true;
            }
        }
        return false;
    }

    public static List<FieldTypeConflict> detectIncompatible(FieldCapabilitiesResponse response, Collection<String> fields) {
        List<FieldTypeConflict> conflicts = new ArrayList<>();
        for (String field : fields) {
            FieldTypeConflict conflict = detectConflictForField(response, field);
            if (conflict != null) {
                conflicts.add(conflict);
            }
        }
        return conflicts;
    }

    private static FieldTypeConflict detectConflictForField(FieldCapabilitiesResponse response, String field) {
        Map<String, FieldCapabilities> typeCaps = response.getField(field);
        if (typeCaps == null || typeCaps.size() <= 1) {
            return null;
        }
        SortedMap<String, SortedSet<String>> typeToProjects = groupTypesToProjects(typeCaps);
        if (typeToProjects.size() <= 1) {
            return null;
        }
        return new FieldTypeConflict(field, typeToProjects);
    }

    private static SortedMap<String, SortedSet<String>> groupTypesToProjects(Map<String, FieldCapabilities> typeCaps) {
        SortedMap<String, SortedSet<String>> typeToProjects = new TreeMap<>();
        for (Map.Entry<String, FieldCapabilities> entry : typeCaps.entrySet()) {
            addProjectsForType(typeToProjects, entry.getKey(), entry.getValue());
        }
        return typeToProjects;
    }

    private static void addProjectsForType(
        SortedMap<String, SortedSet<String>> typeToProjects,
        String type,
        FieldCapabilities capabilities
    ) {
        String[] indices = capabilities.indices();
        if (indices == null || indices.length == 0) {
            return;
        }
        SortedSet<String> projects = typeToProjects.computeIfAbsent(type, ignored -> new TreeSet<>());
        for (String index : indices) {
            projects.add(projectAlias(index));
        }
    }

    public static boolean isIncompatibleTimeField(FieldTypeConflict conflict) {
        return TimeField.TYPES.containsAll(conflict.typeToProjects().keySet()) == false;
    }

    public static boolean isIncompatibleOptionalField(FieldTypeConflict conflict) {
        return areOptionalFieldTypesCompatible(conflict.typeToProjects().keySet()) == false;
    }

    static String formatConflictDetail(FieldTypeConflict conflict) {
        StringBuilder detail = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, SortedSet<String>> entry : conflict.typeToProjects().entrySet()) {
            if (first == false) {
                detail.append(", ");
            }
            first = false;
            detail.append(entry.getKey()).append(" in [").append(String.join(", ", entry.getValue())).append(']');
        }
        return detail.toString();
    }

    public static String optionalFieldWarning(String datafeedId, FieldTypeConflict conflict) {
        return Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_FIELD_TYPE_CONFLICT,
            datafeedId,
            conflict.field(),
            formatConflictDetail(conflict)
        );
    }

    public static String timeFieldConflictError(String datafeedId, String timeField, FieldTypeConflict conflict) {
        return Messages.getMessage(Messages.DATAFEED_TIME_FIELD_TYPE_CONFLICT, datafeedId, timeField, formatConflictDetail(conflict));
    }

    public static String timeFieldProjectExcludedError(
        String datafeedId,
        String timeField,
        String excludedProject,
        FieldTypeConflict conflict
    ) {
        return Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_PROJECT_EXCLUDED_FIELD_CONFLICT,
            datafeedId,
            excludedProject,
            timeField,
            formatConflictDetail(conflict)
        );
    }

    public static FieldTypeConflict removeProjects(FieldTypeConflict conflict, Set<String> projectsToRemove) {
        SortedMap<String, SortedSet<String>> updated = new TreeMap<>();
        for (Map.Entry<String, SortedSet<String>> entry : conflict.typeToProjects().entrySet()) {
            SortedSet<String> remaining = new TreeSet<>(entry.getValue());
            remaining.removeAll(projectsToRemove);
            if (remaining.isEmpty() == false) {
                updated.put(entry.getKey(), remaining);
            }
        }
        if (updated.size() <= 1) {
            return null;
        }
        return new FieldTypeConflict(conflict.field(), updated);
    }

    public static Set<String> projectsInConflict(FieldTypeConflict conflict) {
        Set<String> projects = new LinkedHashSet<>();
        for (SortedSet<String> projectSet : conflict.typeToProjects().values()) {
            projects.addAll(projectSet);
        }
        return projects;
    }

    private static String projectAlias(String index) {
        String alias = RemoteClusterAware.splitIndexName(index).clusterAlias();
        return alias == null ? ORIGIN_PROJECT : alias;
    }
}
