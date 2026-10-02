/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks active cross-project field-type conflicts for a running datafeed and emits deltas only when
 * conflict signatures change.
 */
public final class DatafeedFieldConflictTracker {

    public record TrackerDelta(
        List<DatafeedFieldConflictDiagnostics.FieldTypeConflict> newOrChangedConflicts,
        List<String> resolvedFields
    ) {
        static final TrackerDelta EMPTY = new TrackerDelta(List.of(), List.of());

        public boolean isEmpty() {
            return newOrChangedConflicts.isEmpty() && resolvedFields.isEmpty();
        }
    }

    private final Map<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> activeConflicts = new HashMap<>();
    private final Set<String> timeFieldExcludedProjects = new HashSet<>();

    public TrackerDelta applyRecheck(FieldCapabilitiesResponse response, Collection<String> fields, String timeField) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(fields);
        Objects.requireNonNull(timeField);

        Map<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> detected = new HashMap<>();
        for (DatafeedFieldConflictDiagnostics.FieldTypeConflict conflict : DatafeedFieldConflictDiagnostics.detectIncompatible(
            response,
            fields
        )) {
            if (conflict.field().equals(timeField)) {
                if (DatafeedFieldConflictDiagnostics.isIncompatibleTimeField(conflict)) {
                    detected.put(conflict.field(), conflict);
                }
            } else if (DatafeedFieldConflictDiagnostics.isIncompatibleOptionalField(conflict)) {
                detected.put(conflict.field(), conflict);
            }
        }
        return diffAgainstActive(detected);
    }

    public TrackerDelta handleUnlink(Set<String> unlinkedProjects) {
        if (unlinkedProjects.isEmpty()) {
            return TrackerDelta.EMPTY;
        }
        Map<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> updated = new HashMap<>();
        for (Map.Entry<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> entry : activeConflicts.entrySet()) {
            DatafeedFieldConflictDiagnostics.FieldTypeConflict remaining = DatafeedFieldConflictDiagnostics.removeProjects(
                entry.getValue(),
                unlinkedProjects
            );
            if (remaining != null) {
                updated.put(entry.getKey(), remaining);
            }
        }
        unlinkedProjects.forEach(timeFieldExcludedProjects::remove);
        return diffAgainstActive(updated);
    }

    public Set<String> timeFieldExcludedProjects() {
        return Set.copyOf(timeFieldExcludedProjects);
    }

    public void markProjectExcludedForTimeFieldConflict(String projectAlias) {
        timeFieldExcludedProjects.add(projectAlias);
    }

    public void markProjectIncludedForTimeFieldConflict(String projectAlias) {
        timeFieldExcludedProjects.remove(projectAlias);
    }

    private TrackerDelta diffAgainstActive(Map<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> detected) {
        List<DatafeedFieldConflictDiagnostics.FieldTypeConflict> newOrChanged = new ArrayList<>();
        List<String> resolved = new ArrayList<>();

        for (Map.Entry<String, DatafeedFieldConflictDiagnostics.FieldTypeConflict> entry : detected.entrySet()) {
            DatafeedFieldConflictDiagnostics.FieldTypeConflict previous = activeConflicts.get(entry.getKey());
            if (previous == null || previous.equals(entry.getValue()) == false) {
                newOrChanged.add(entry.getValue());
            }
        }

        for (String field : activeConflicts.keySet()) {
            if (detected.containsKey(field) == false) {
                resolved.add(field);
            }
        }

        activeConflicts.clear();
        activeConflicts.putAll(detected);
        return new TrackerDelta(List.copyOf(newOrChanged), List.copyOf(resolved));
    }
}
